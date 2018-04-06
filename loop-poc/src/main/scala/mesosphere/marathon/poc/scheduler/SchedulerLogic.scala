package mesosphere.marathon
package poc.scheduler

import akka.NotUsed
import akka.stream.scaladsl.Flow
import java.time.{ Clock, Instant }
import java.util.UUID
import mesosphere.marathon.poc.repository.{ StateFrame, StateTransition }
import mesosphere.marathon.poc.repository.MarathonState
import mesosphere.marathon.poc.state.Instance
import org.apache.mesos.v1.mesos.{ Offer, TaskState, TaskStatus }
import monocle.macros.syntax.lens._
import scala.annotation.tailrec

case class MesosTask(
    taskId: String,
    agentId: String,
    phase: MesosTask.Phase) {

  val (name, instanceId, incarnation) = Instance.parseMesosTaskId(taskId) match {
    case Some(tpl) => tpl
    case None =>
      // shouldn't really get here
      ("???", MesosTask.emptyUUID, 0L)
  }
}

object MesosTask {
  private[MesosTask] val emptyUUID = UUID.fromString("00000000-0000-0000-0000-000000000000")

  def considerRunning(phase: Phase) = phase match {
    case _: Phase.Launching | _: Phase.Running => true
    case _: Phase.Killing | _: Phase.Terminal => false
  }
  def considerStopped(phase: Phase) = phase match {
    case _: Phase.Launching | _: Phase.Running => false
    case _: Phase.Killing | _: Phase.Terminal => true
  }

  def apply(mesosTask: TaskStatus): Option[MesosTask] =
    mesosTask.agentId.value map { agentId =>
      MesosTask(
        mesosTask.taskId.value,
        agentId.value,
        Phase(mesosTask))
    }

  /**
    * LOL don't read this too closely. just needed something quick to model task life-cycle
    */
  sealed trait Phase
  object Phase {
    def apply(taskStatus: TaskStatus): Phase = {
      import TaskState._
      val timestamp = Instant.ofEpochMilli((taskStatus.timestamp.getOrElse(0.0) * 1000).toLong)

      taskStatus.state match {
        case TASK_STAGING | TASK_STARTING | TASK_RUNNING
          | TASK_UNREACHABLE | TASK_LOST | TASK_UNKNOWN =>
          Running(taskStatus)
        case  TASK_KILLING =>
          Killing(timestamp, taskStatus)
        case TASK_FINISHED | TASK_FAILED | TASK_KILLED | TASK_ERROR | TASK_DROPPED | TASK_GONE | TASK_GONE_BY_OPERATOR =>
          Terminal(timestamp, taskStatus)
      }
    }

    case class Launching(timestamp: Instant) extends Phase
    /**
      * We have received a MesosStatus for this task.
      */
    case class Running(
        status: TaskStatus) extends Phase

    /** We're killing this task */
    case class Killing(
        lastkilledAt: Instant,
        status: TaskStatus) extends Phase

    case class Terminal(
        timestamp: Instant,
        status: TaskStatus) extends Phase
  }
}

case class MesosAgent(
    id: String,
    hostname: String)

case class MesosState(
    agents: Map[String, MesosAgent],
    tasks: Map[String, MesosTask])

object MesosState {
  def empty = MesosState(Map.empty, Map.empty)
}

sealed trait SchedulerLogicInputEvent
object SchedulerLogicInputEvent {
  case class MarathonStateUpdate(updates: Seq[StateTransition]) extends SchedulerLogicInputEvent
  case class MesosOffer(offer: Offer) extends SchedulerLogicInputEvent
  case class MesosTaskStatus(task: MesosTask) extends SchedulerLogicInputEvent
}

case class SchedulerFrame(
    state: MarathonState,
    mesosState: MesosState)

object SchedulerFrame {
  def empty = SchedulerFrame(state = MarathonState.empty, MesosState.empty)
}

object SchedulerLogic {
  sealed trait Effect
  sealed trait MesosEffect extends Effect
  object Effect {
    /**
      * Indicate that an instance wants offers (so it can launch)
      *
      * for multi-role could include the role here so downstream Mesos can update subscription accordingly
      */
    case class WantOffers(instanceId: UUID) extends MesosEffect

    /**
      * Indicate to Mesos that a task should be killed
      */
    case class KillTask(taskId: String, agent: Option[String]) extends MesosEffect

    /**
      * Indicate that the task is to be forgotten.
      */
    case class ExpungeTask(taskId: String) extends Effect
    case class BumpIncarnation(instanceId: UUID, incarnation: Long) extends Effect
  }

  def computeEffect(marathonInstance: Option[Instance], mesosTask: Option[MesosTask]): Seq[Effect] = (marathonInstance, mesosTask) match {
    case (Some(instance), None) =>
      Seq(Effect.WantOffers(instance.instanceId))
    case (Some(instance), Some(task)) if task.incarnation < instance.incarnation =>
      if (MesosTask.considerRunning(task.phase))
        Seq(Effect.KillTask(task.taskId, Some(task.agentId)))
      else
        Seq(
          Effect.KillTask(task.taskId, Some(task.agentId)),
          Effect.ExpungeTask(task.taskId))
    case (None, Some(task)) =>
      if (MesosTask.considerRunning(task.phase))
        Seq(
          Effect.KillTask(task.taskId, Some(task.agentId)),
          Effect.ExpungeTask(task.taskId))
      else
        Nil
    case (Some(instance), Some(task)) if task.incarnation == instance.incarnation =>
      (instance.goal, MesosTask.considerRunning(task.phase)) match {
        case (Instance.Goal.Running, true) =>
          Nil // notify orchestrator ?
        case (Instance.Goal.Running, false) =>
          // ... via orchestrator? will potentially need to rate-limit launches
          Seq(Effect.BumpIncarnation(instance.instanceId, instance.incarnation + 1))
        case (Instance.Goal.Stopped, true) =>
          Seq(Effect.KillTask(task.taskId, Some(task.agentId)))
        case (Instance.Goal.Stopped, false) =>
          Nil // notify orchestrator?
      }
    case (Some(instance), Some(task)) if task.incarnation > instance.incarnation =>
      /* We should never get here. */

      if (MesosTask.considerRunning(task.phase))
        Seq(
          Effect.KillTask(task.taskId, Some(task.agentId)),
          Effect.ExpungeTask(task.taskId),
          Effect.BumpIncarnation(instance.instanceId, task.incarnation + 1))
      else
        Nil
  }

  def computeEffects(marathonState: MarathonState, mesosState: MesosState, affectedInstances: Seq[UUID]): List[Effect] = {
    affectedInstances.distinct.flatMap { instanceId =>
      computeEffect(marathonState.instances.instances.get(instanceId), mesosState.tasks.get(instanceId.toString))
    }(collection.breakOut)
  }

  def applyEffectsToMesos(mesosState: MesosState, effects: List[Effect])(clock: Clock): MesosState =
    effects.foldLeft(mesosState) { (mesosState, effect) =>
      effect match {
        case Effect.ExpungeTask(taskId) =>
          mesosState.lens(_.tasks).modify { _ - taskId }
        case Effect.KillTask(taskId, _) =>
          import monocle.function.At.at
          mesosState.lens(_.tasks).composeLens(at(taskId)).modify {
            case Some(task) =>
              task.phase match {
                case MesosTask.Phase.Running(status) =>
                  Some(task.copy(phase = MesosTask.Phase.Killing(clock.instant(), status)))
                case other =>
                  throw new IllegalStateException(s"BUG! Tried to kill a non-running task, ${task}")
              }
            case None =>
              throw new IllegalStateException(s"BUG! Tried to kill a non-existent task, ${taskId}")
          }
        case _ =>
          mesosState
      }
    }

  def eventProcesorFlow(clock: Clock = Clock.systemUTC()): Flow[SchedulerLogicInputEvent, Effect, NotUsed] =
    Flow[SchedulerLogicInputEvent].statefulMapConcat { () =>
      var frame = SchedulerFrame.empty
      val wantingOffers = collection.mutable.Set.empty[UUID]

      { inputEvent =>

        val effects = inputEvent match {
          case SchedulerLogicInputEvent.MarathonStateUpdate(updates) =>
            val nextMarathonState = StateTransition.applyTransitions(frame.state, updates)
            val effects = computeEffects(nextMarathonState, frame.mesosState, StateTransition.affectedInstanceIds(updates))
            val provisionalMesosState = applyEffectsToMesos(frame.mesosState, effects)(clock)
            frame = frame.copy(nextMarathonState, provisionalMesosState)

            effects
        }

        // update wantingOffers
        effects.foreach {
          case e: Effect.WantOffers =>
            wantingOffers += e.instanceId
          /* case e: Effect.LaunchTask(instanceId, _) => wantingOffers -= instanceId */
          case _ =>
        }

        effects
      }
    }

  /**
    * Given Mesos
    */
  def shouldRevive(frame: StateFrame) = {

  }
}
