package mesosphere.marathon
package scheduler

import akka.NotUsed
import akka.stream.scaladsl.Flow
import java.time.Instant
import mesosphere.marathon.repository.{ Frame, StateTransition }

case class MesosTask(
  taskId: String,
  lol: Int)
sealed trait OffersState
case object Suppressed extends OffersState
case class Revived(when: Instant) extends OffersState


case class MesosState(
  tasks: Map[String, MesosTask],
  offersState: OffersState
)

sealed trait SchedulerLogicInputEvent
object SchedulerLogicInputEvent {
  case class MarathonStateUpdate(updates: Seq[StateTransition]) extends SchedulerLogicInputEvent
}

object SchedulerLogic {
  val eventProcesorFlow: Flow[SchedulerLogicInputEvent, Nothing, NotUsed] =
    Flow[SchedulerLogicInputEvent].statefulMapConcat { () =>
      var frame = Frame.empty

      {
        case SchedulerLogicInputEvent.MarathonStateUpdate(updates) =>
          val nextFrame = StateTransition.applyTransitions(frame, updates)
          // compute effects

          List()
      }
    }


  /**
    * Given Mesos
    */
  def shouldRevive(frame: Frame) = {

  }
}
