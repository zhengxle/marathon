package mesosphere.marathon
package core.task.termination.impl

import java.time.Clock

import akka.Done
import akka.actor.ActorRef
import com.typesafe.scalalogging.StrictLogging
import mesosphere.marathon.core.instance.Instance
import mesosphere.marathon.core.instance.update.InstanceUpdateOperation
import mesosphere.marathon.core.task.Task
import mesosphere.marathon.core.task.termination.{KillReason, KillService}
import mesosphere.marathon.core.task.tracker.InstanceTracker

import scala.concurrent.{Future, Promise}
import scala.collection.immutable.Seq
import scala.async.Async.{async, await}

private[termination] class KillServiceDelegate(
    actorRef: ActorRef, instanceTracker: InstanceTracker, clock: Clock) extends KillService with StrictLogging {
  import KillServiceActor._

  override def killInstances(instances: Seq[Instance], reason: KillReason): Future[Done] = {
    logger.info(
      s"Killing ${instances.size} tasks for reason: $reason (ids: {} ...)",
      instances.take(5).map(_.instanceId).mkString(","))

    // TODO: if we can't update an instance to Stopping we should log an error and not kill it
    // That should bubble up to the API. Or maybe the Future should fail in this case?
    import scala.concurrent.ExecutionContext.Implicits.global
    async {
      await {
        Future.sequence(instances.map { instance =>
          instanceTracker.process(InstanceUpdateOperation.Decommission(instance.instanceId, clock.now()))
        }).map { updateEffects =>
          logger.info(s">>>>> Decommissioning instances. updateEffects: ${updateEffects.map(_.name)}")
          Done
        }
      }
      logger.info(s">>>>> transitioned instances to Decommissioned")
    }

    val promise = Promise[Done]
    actorRef ! KillInstances(instances, promise)

    promise.future
  }

  override def killInstance(instance: Instance, reason: KillReason): Future[Done] = {
    killInstances(Seq(instance), reason)
  }

  override def killUnknownTask(taskId: Task.Id, reason: KillReason): Unit = {
    logger.info(s"Killing unknown task for reason: $reason (id: {})", taskId)
    actorRef ! KillUnknownTaskById(taskId)
  }
}
