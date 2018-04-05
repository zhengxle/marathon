package mesosphere.marathon
package poc.repository

import mesosphere.marathon.poc.state.{ Instance, RunSpec, RunSpecRef }
import java.util.UUID
import monocle.macros.syntax.lens._

sealed trait StateTransition
object StateTransition {
  case class RunSpecUpdated(ref: RunSpecRef, runSpec: Option[RunSpec]) extends StateTransition
  case class InstanceUpdated(instanceId: UUID, instance: Option[Instance]) extends StateTransition

  // extract and share with Marathon scheduler
  def applyTransitions(frame: MarathonState, effects: Seq[StateTransition]): MarathonState = {
    effects.foldLeft(frame) {
      case (frame, update: InstanceUpdated) =>
        update.instance match {
          case Some(instance) =>
            frame.lens(_.instances).modify(_.withInstance(instance))
          case None =>
            frame.lens(_.instances).modify(_.withoutInstance(update.instanceId))
        }
      case (frame, update: RunSpecUpdated) =>
        update.runSpec match {
          case Some(runSpec) =>
            frame.lens(_.rootGroup).modify(_.withApp(runSpec))
          case None =>
            frame.lens(_.rootGroup).modify(_.withoutApp(update.ref))
        }
    }
  }

  /**
    * Given a series of state transitions, return all items for which the instance id had changed
    */
  def affectedInstanceIds(transitions: Seq[StateTransition]): Seq[UUID] = transitions.collect {
    case InstanceUpdated(instanceId, _) => instanceId
  }
}
