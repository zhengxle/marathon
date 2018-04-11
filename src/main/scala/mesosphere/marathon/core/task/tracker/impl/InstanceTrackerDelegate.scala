package mesosphere.marathon
package core.task.tracker.impl

import java.time.Clock
import java.util.concurrent.TimeoutException

import akka.Done
import akka.actor.ActorRef
import akka.pattern.{AskTimeoutException, ask}
import akka.util.Timeout
import mesosphere.marathon.core.instance.Instance
import mesosphere.marathon.core.instance.update.{InstanceUpdateEffect, InstanceUpdateOperation}
import mesosphere.marathon.core.task.tracker.impl.InstanceTrackerActor.ForwardTaskOp
import mesosphere.marathon.core.task.tracker.{InstanceTracker, InstanceTrackerConfig}
import mesosphere.marathon.metrics.{Metrics, ServiceMetric}
import mesosphere.marathon.state.{PathId, Timestamp}
import org.apache.mesos

import scala.concurrent.duration._
import scala.concurrent.{Await, ExecutionContext, Future}
import scala.util.control.NonFatal

/**
  * Provides a [[InstanceTracker]] interface to [[InstanceTrackerActor]].
  *
  * This is used for the "global" TaskTracker trait and it is also
  * is used internally in this package to communicate with the TaskTracker.
  */
private[tracker] class InstanceTrackerDelegate(
    clock: Clock,
    config: InstanceTrackerConfig,
    taskTrackerRef: ActorRef) extends InstanceTracker {

  private[impl] implicit val timeout: Timeout = config.internalTaskUpdateRequestTimeout().milliseconds

  override def instancesBySpecSync: InstanceTracker.InstancesBySpec = {
    import scala.concurrent.ExecutionContext.Implicits.global
    Await.result(instancesBySpec(), taskTrackerQueryTimeout.duration)
  }

  override def instancesBySpec()(implicit ec: ExecutionContext): Future[InstanceTracker.InstancesBySpec] = tasksByAppTimer {
    (taskTrackerRef ? InstanceTrackerActor.List).mapTo[InstanceTracker.InstancesBySpec].recover {
      case e: AskTimeoutException =>
        throw new TimeoutException(
          "timeout while calling list. If you know what you are doing, you can adjust the timeout " +
            s"with --${config.internalTaskTrackerRequestTimeout.name}."
        )
    }
  }

  // TODO(jdef) support pods when counting launched instances
  override def countActiveSpecInstances(appId: PathId): Future[Int] = {
    import scala.concurrent.ExecutionContext.Implicits.global
    instancesBySpec().map(_.specInstances(appId).count(instance => instance.isActive || instance.isReserved))
  }

  override def hasSpecInstancesSync(appId: PathId): Boolean = instancesBySpecSync.hasSpecInstances(appId)
  override def hasSpecInstances(appId: PathId)(implicit ec: ExecutionContext): Future[Boolean] =
    instancesBySpec().map(_.hasSpecInstances(appId))

  override def specInstancesSync(appId: PathId): Seq[Instance] =
    instancesBySpecSync.specInstances(appId)
  override def specInstances(appId: PathId)(implicit ec: ExecutionContext): Future[Seq[Instance]] =
    instancesBySpec().map(_.specInstances(appId))

  override def instance(taskId: Instance.Id): Future[Option[Instance]] =
    (taskTrackerRef ? InstanceTrackerActor.Get(taskId)).mapTo[Option[Instance]]

  private[this] val tasksByAppTimer = Metrics.timer(ServiceMetric, getClass, "tasksByApp")

  private[this] implicit val taskTrackerQueryTimeout: Timeout = config.internalTaskTrackerRequestTimeout().milliseconds

  override def process(stateOp: InstanceUpdateOperation): Future[InstanceUpdateEffect] = {
    import akka.pattern.ask

    val instanceId: Instance.Id = stateOp.instanceId
    val deadline = clock.now + timeout.duration
    val op: ForwardTaskOp = InstanceTrackerActor.ForwardTaskOp(deadline, instanceId, stateOp)
    (taskTrackerRef ? op).mapTo[InstanceUpdateEffect].recover {
      case NonFatal(e) =>
        throw new RuntimeException(s"while asking for $op on runSpec [${instanceId.runSpecId}] and $instanceId", e)
    }
  }

  override def launchEphemeral(instance: Instance): Future[Done] = {
    process(InstanceUpdateOperation.LaunchEphemeral(instance)).map(_ => Done)
  }

  override def revert(instance: Instance): Future[Done] = {
    process(InstanceUpdateOperation.Revert(instance)).map(_ => Done)
  }

  override def forceExpunge(instanceId: Instance.Id): Future[Done] = {
    process(InstanceUpdateOperation.ForceExpunge(instanceId)).map(_ => Done)
  }

  override def updateStatus(instance: Instance, mesosStatus: mesos.Protos.TaskStatus, updateTime: Timestamp): Future[Done] = {
    process(InstanceUpdateOperation.MesosUpdate(instance, mesosStatus, updateTime)).map(_ => Done)
  }

  override def updateReservationTimeout(instanceId: Instance.Id): Future[Done] = {
    process(InstanceUpdateOperation.ReservationTimeout(instanceId)).map(_ => Done)
  }
}
