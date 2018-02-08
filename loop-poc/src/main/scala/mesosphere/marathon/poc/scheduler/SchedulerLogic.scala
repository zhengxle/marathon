package mesosphere.marathon
package poc.scheduler

import akka.NotUsed
import akka.stream.scaladsl.Flow
import java.time.Instant
import mesosphere.marathon.poc.repository.{ StateFrame, StateTransition }
import mesosphere.marathon.poc.repository.MarathonState

case class MesosTask(
  taskId: String,
  lol:    Int)

case class MesosAgent(
    id: String,
    hostname: String)

case class MesosState(
  agents: Map[String, MesosAgent],
  tasks:  Map[String, MesosTask])

object MesosState {
  def empty = MesosState(Map.empty, Map.empty)
}

sealed trait SchedulerLogicInputEvent
object SchedulerLogicInputEvent {
  case class MarathonStateUpdate(updates: Seq[StateTransition]) extends SchedulerLogicInputEvent
}

case class SchedulerFrame(
  state: MarathonState,
  mesosState: MesosState)

object SchedulerFrame {
  def empty = SchedulerFrame(state = MarathonState.empty, MesosState.empty)
}

object SchedulerLogic {
  def computeEffects(marathonState: MarathonState, mesosState: MesosState)
  val eventProcesorFlow: Flow[SchedulerLogicInputEvent, Nothing, NotUsed] =
    Flow[SchedulerLogicInputEvent].statefulMapConcat { () =>
      var frame = SchedulerFrame.empty

      {
        case SchedulerLogicInputEvent.MarathonStateUpdate(updates) =>
          val nextState = StateTransition.applyTransitions(frame.state, updates)
          // compute effects


          List()
      }
    }

  /**
   * Given Mesos
   */
  def shouldRevive(frame: StateFrame) = {

  }
}
