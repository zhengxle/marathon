package mesosphere.marathon
package poc.repository

import akka.stream.scaladsl.Flow
import mesosphere.marathon.poc.state.{ RunSpec, Instance }
import monocle.macros.syntax.lens._

object StateAuthority {
  sealed trait StateAuthorityInputEvent

  case class CommandRequest(requestId: Long, command: StateCommand) extends StateAuthorityInputEvent

  sealed trait StateCommand
  object StateCommand {
    case class PutApp(runSpec: RunSpec) extends StateCommand

    case class AddInstance(instance: Instance) extends StateCommand
  }

  sealed trait Effect
  object Effect {
    // reason ideally is modeled with more detail than string
    case class CommandFailure(requestId: Long, rejection: Rejection) extends Effect
    case class StateUpdated(requestId: Long, updates: Seq[StateTransition]) extends Effect
  }

  case class Rejection(reason: String)

  val commandProcessorFlow = Flow[StateAuthorityInputEvent].statefulMapConcat { () =>
    var currentFrame: StateFrame = StateFrame.empty

    { event =>

      val (effects, nextFrame) = StateAuthority.submitEvent(currentFrame, event)
      currentFrame = nextFrame
      effects
    }
  }

  /**
    * Given a command and a requestId, return some effects and the next frame
    */
  def submitEvent(frame: StateFrame, event: StateAuthorityInputEvent): (Seq[Effect], StateFrame) = event match {
    case CommandRequest(requestId, command) =>
      applyCommand(frame, command) match {
        case result @ Left(rejection) =>
          // issue failure for requestId
          (
            List(Effect.CommandFailure(requestId, rejection)),
            frame)
        case Right(stateTransitions) =>

          //    frameWithUpdate.lens(_.version).modify(_ + 1)

          val nextFrame = frame
            .lens(_.state).modify(StateTransition.applyTransitions(_, stateTransitions))

          (
            List(Effect.StateUpdated(requestId, stateTransitions)),
            nextFrame)
      }
  }

  def applyCommand(frame: StateFrame, command: StateCommand): Either[Rejection, Seq[StateTransition]] = {
    command match {
      case addApp: StateCommand.PutApp =>
        // we'd apply a validation here
        Right(
          Seq(
            StateTransition.RunSpecUpdated(ref = addApp.runSpec.ref, runSpec = Some(addApp.runSpec))))
      case addInstance: StateCommand.AddInstance =>
        if (frame.state.rootGroup.get(addInstance.instance.runSpec).isEmpty)
          Left(
            Rejection(s"No runSpec ${addInstance.instance.runSpec}"))
        else
          Right(
            Seq(
              StateTransition.InstanceUpdated(addInstance.instance.instanceId, Some(addInstance.instance))))

    }
  }
}
