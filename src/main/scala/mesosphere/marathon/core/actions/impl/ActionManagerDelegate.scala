package mesosphere.marathon
package core.actions.impl

import akka.actor.ActorRef
import akka.pattern.ask
import akka.util.Timeout
import com.typesafe.scalalogging.StrictLogging
import mesosphere.marathon.core.actions.impl.ActionManagerActor.ActionHandlerResult
import mesosphere.marathon.core.actions.{ Action, ActionManager }

import scala.concurrent.duration._
import scala.concurrent.{ Await, Future }
import scala.reflect.ClassTag
import scala.util.control.NonFatal

private[actions] class ActionManagerDelegate(
    actorRef: ActorRef) extends ActionManager with StrictLogging {

  // When purging, we wait for the TaskLauncherActor to shut down. This actor will wait for
  // in-flight task op notifications before complying, therefore we need to adjust the timeout accordingly.
  val purgeTimeout: Timeout = DurationInt(3000).milliseconds + DurationInt(30000).millisecond

  val launchQueueRequestTimeout: Timeout = DurationInt(3000).milliseconds

  override def add(action: Action): ActionHandlerResult =
    askActionActor[ActionManagerDelegate.Request, ActionHandlerResult]("add")(ActionManagerDelegate.Add(action))

  override def addAsync(action: Action): Future[ActionHandlerResult] =
    askActionActorFuture[ActionManagerDelegate.Request, ActionHandlerResult]("add")(ActionManagerDelegate.Add(action))

  private[this] def askActionActor[T, R: ClassTag](
    method: String,
    timeout: Timeout = launchQueueRequestTimeout)(message: T): R = {

    val answerFuture = askActionActorFuture[T, R](method, timeout)(message)
    Await.result(answerFuture, timeout.duration)
  }

  private[this] def askActionActorFuture[T, R: ClassTag](
    method: String,
    timeout: Timeout = launchQueueRequestTimeout)(message: T): Future[R] = {

    implicit val timeoutImplicit: Timeout = timeout
    val answerFuture = actorRef ? message
    import mesosphere.marathon.core.async.ExecutionContexts.global
    answerFuture.recover {
      case NonFatal(e) => throw new RuntimeException(s"in $method", e)
    }
    answerFuture.mapTo[R]
  }
}

private[actions] object ActionManagerDelegate {
  sealed trait Request
  case class Add(action: Action) extends Request
}
