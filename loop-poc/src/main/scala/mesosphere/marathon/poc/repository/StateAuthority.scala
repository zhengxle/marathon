package mesosphere.marathon
package poc.repository

import akka.stream.scaladsl.Flow
import mesosphere.marathon.poc.state.{ RunSpec, Instance }
import monocle.macros.syntax.lens._

object StateAuthority {
  sealed trait StateAuthorityInputEvent

  /**
    * Notify that a version is persisted. Should only be submitted by storage component.
    */
  case class MarkPersisted(version: Long) extends StateAuthorityInputEvent

  case class CommandRequest(requestId: Long, command: StateCommand) extends StateAuthorityInputEvent

  sealed trait StateCommand
  object StateCommand {
    case class PutApp(runSpec: RunSpec) extends StateCommand

    case class AddInstance(instance: Instance) extends StateCommand
  }

  sealed trait Effect
  object Effect {
    // reason ideally is modeled with more detail than string
    case class PublishResult(requestId: Long, result: Either[Rejection, Result]) extends Effect
    case class PersistUpdates(version: Long, updates: Seq[StateTransition]) extends Effect
    case class PublishUpdates(updates: Seq[StateTransition]) extends Effect
  }

  case class Rejection(reason: String)
  case class Result(
      stateTransitions: Seq[StateTransition])

  val commandProcessorFlow = Flow[StateAuthorityInputEvent].statefulMapConcat { () =>
    var currentFrame: StateFrame = StateFrame.empty

    { event =>

      val (effects, nextFrame) = StateAuthority.submitEvent(currentFrame, event)
      currentFrame = nextFrame
      effects
    }
  }

  /**
    * Use to prevent log jam. If the state authority stage is not accepting events because it is blocked reporting back
    * MarkPersisted is back-pressuring the storage mechanism, which is back-pressuring the state authority stage, we can
    * endlessly pull MarkPersisted events and collapse them together.
    */
  val markPersistedConflator = Flow[MarkPersisted].conflate { (a, b) => MarkPersisted(Math.max(a.version, b.version)) }

  /**
    * Given a command and a requestId, return some effects and the next frame
    */
  def submitEvent(frame: StateFrame, event: StateAuthorityInputEvent): (Seq[Effect], StateFrame) = event match {
    case CommandRequest(requestId, command) =>
      applyCommand(frame, command) match {
        case result @ Left(failure) =>
          // issue failure for requestId
          (
            List(Effect.PublishResult(requestId, result)),
            frame)
        case Right(result) =>

          //    frameWithUpdate.lens(_.version).modify(_ + 1)

          val nextFrame = frame
            .lens(_.version).modify(_ + 1)
            .lens(_.state).modify(StateTransition.applyTransitions(_, result.stateTransitions))
          val withUpdates = nextFrame.lens(_.pendingUpdates).modify { pendingUpdates =>
            pendingUpdates.enqueue(PendingUpdate(nextFrame.version, requestId, result.stateTransitions))
          }

          (
            List(Effect.PersistUpdates(nextFrame.version, result.stateTransitions)),
            withUpdates)
      }
    case MarkPersisted(version) =>
      val updates = frame.pendingUpdates.iterator.takeWhile { _.version <= version }.toList
      val nextFrame = frame.lens(_.pendingUpdates).modify(_.drop(updates.size))

      val effects: List[Effect] = Effect.PublishUpdates(updates.flatMap(_.updates)) ::
        updates.map { u => Effect.PublishResult(u.requestId, Right(Result(u.updates))) }

      (effects, nextFrame)
  }

  def applyCommand(frame: StateFrame, command: StateCommand): Either[Rejection, Result] = {
    command match {
      case addApp: StateCommand.PutApp =>
        // we'd apply a validation here
        Right(
          Result(
            Seq(
              StateTransition.RunSpecUpdated(ref = addApp.runSpec.ref, runSpec = Some(addApp.runSpec)))))
      case addInstance: StateCommand.AddInstance =>
        if (frame.state.rootGroup.get(addInstance.instance.runSpec).isEmpty)
          Left(
            Rejection(s"No runSpec ${addInstance.instance.runSpec}"))
        else
          Right(
            Result(
              Seq(
                StateTransition.InstanceUpdated(addInstance.instance.instanceId, Some(addInstance.instance)))))

    }
  }
}
