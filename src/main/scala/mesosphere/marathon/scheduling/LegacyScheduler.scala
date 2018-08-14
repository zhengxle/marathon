package mesosphere.marathon
package scheduling

import akka.Done
import mesosphere.marathon.core.instance.{Goal, Instance}
import mesosphere.marathon.core.launcher.OfferProcessor
import mesosphere.marathon.core.launchqueue.LaunchQueue
import mesosphere.marathon.core.task.tracker.InstanceTracker
import mesosphere.marathon.core.task.update.TaskStatusUpdateProcessor
import mesosphere.marathon.state.{PathId, RunSpec}
import org.apache.mesos.Protos

import scala.async.Async.{async, await}
import scala.concurrent.{ExecutionContext, Future}

case class LegacyScheduler(
    offerProcessor: OfferProcessor,
    instanceTracker: InstanceTracker,
    statusUpdateProcessor: TaskStatusUpdateProcessor,
    launchQueue: LaunchQueue) extends Scheduler {

  override def add(runSpec: RunSpec, count: Int)(implicit ec: ExecutionContext): Future[Done] = async {
    // Update run spec in task launcher actor.
    // Currently the [[TaskLauncherActor]] always starts instances with the latest run spec. Let's say there are 2
    // running instances with v1 and 3 scheduled for v1. If the users forces an update to v2 the current logic will
    // kill the 2 running instances and only tell the [[TaskLauncherActor]] to start the 3 scheduled v1 instances with
    // the v2 run spec. We then schedule 2 more v2 instances. In the future we probably want to bind instances to a
    // certain run spec. Until then we have to update the run spec in a [[TaskLauncherActor]]
    await(launchQueue.sync(runSpec))

    await(launchQueue.add(runSpec, count))

    launchQueue.resetDelay(runSpec)

    Done
  }

  override def getInstances(runSpecId: PathId)(implicit ec: ExecutionContext): Future[Seq[Instance]] = instanceTracker.specInstances(runSpecId)

  override def getInstance(instanceId: Instance.Id)(implicit ec: ExecutionContext): Future[Option[Instance]] = instanceTracker.get(instanceId)

  @SuppressWarnings(Array("all")) // async/await
  override def run(instanceIds: Instance.Id*)(implicit ec: ExecutionContext): Future[Done] = async {
    val work = Future.sequence(instanceIds.map(instanceTracker.setGoal(_, Goal.Running)))
    await(work)
    Done
  }

  @SuppressWarnings(Array("all")) // async/await
  override def decommission(instanceIds: Instance.Id*)(implicit ec: ExecutionContext): Future[Done] = async {
    val work = Future.sequence(instanceIds.map(instanceTracker.setGoal(_, Goal.Decommissioned)))
    await(work)
    Done
  }

  @SuppressWarnings(Array("all")) // async/await
  override def stop(instanceIds: Instance.Id*)(implicit ec: ExecutionContext): Future[Done] = async {
    val work = Future.sequence(instanceIds.map(instanceTracker.setGoal(_, Goal.Stopped)))
    await(work)
    Done
  }

  override def processOffer(offer: Protos.Offer): Future[Done] = offerProcessor.processOffer(offer)

  override def processMesosUpdate(status: Protos.TaskStatus)(implicit ec: ExecutionContext): Future[Done] = statusUpdateProcessor.publish(status).map(_ => Done)
}
