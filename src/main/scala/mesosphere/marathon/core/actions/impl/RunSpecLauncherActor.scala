package mesosphere.marathon
package core.actions.impl

import java.util.UUID

import akka.Done
import akka.actor._
import akka.event.LoggingReceive
import com.typesafe.scalalogging.StrictLogging
import mesosphere.marathon.core.base.Clock
import mesosphere.marathon.core.flow.OfferReviver
import mesosphere.marathon.core.instance.Instance
import mesosphere.marathon.core.instance.update.{ InstanceChange, InstanceDeleted, InstanceUpdated }
import mesosphere.marathon.core.launcher.{ InstanceOp, InstanceOpFactory, OfferMatchResult }
import mesosphere.marathon.core.matcher.base.OfferMatcher
import mesosphere.marathon.core.matcher.base.OfferMatcher.{ InstanceOpWithSource, MatchedInstanceOps }
import mesosphere.marathon.core.matcher.base.util.{ ActorOfferMatcher, InstanceOpSourceDelegate }
import mesosphere.marathon.core.matcher.manager.OfferMatcherManager
import mesosphere.marathon.core.task.tracker.InstanceTracker
import mesosphere.marathon.state.{ PathId, RunSpec }
import org.apache.mesos.{ Protos => Mesos }
import mesosphere.marathon.stream.Implicits._

import scala.collection.mutable
import scala.concurrent.Promise
import scala.concurrent.duration._

private[actions] object RunSpecLauncherActor {
  def props(
    offerMatcherManager: OfferMatcherManager,
    clock: Clock,
    taskOpFactory: InstanceOpFactory,
    maybeOfferReviver: Option[OfferReviver],
    instanceTracker: InstanceTracker
  ): Props = {
    Props(new RunSpecLauncherActor(
      offerMatcherManager,
      clock, taskOpFactory,
      maybeOfferReviver,
      instanceTracker))
  }

  sealed trait Requests

  /**
    * Increase the instance count of the receiver.
    * The actor responds with an [[InstanceCreated]] message.
    */
  case class AddInstance(spec: RunSpec, restartOnExit: Boolean, restartOnFailure: Boolean, id: Option[UUID]) extends Requests

  case class InstanceCreated(instanceId: Instance.Id, id: Option[UUID]) extends Requests

  private val OfferOperationRejectedTimeoutReason: String =
    "InstanceLauncherActor: no accept received within timeout. " +
      "You can reconfigure the timeout with --task_operation_notification_timeout."
}

/**
  * Allows processing offers for starting tasks for the given app.
  */
private class RunSpecLauncherActor(
    offerMatcherManager: OfferMatcherManager,
    clock: Clock,
    instanceOpFactory: InstanceOpFactory,
    maybeOfferReviver: Option[OfferReviver],
    instanceTracker: InstanceTracker
) extends Actor with StrictLogging with Stash {
  // scalastyle:on parameter.number

  private[this] var inFlightInstanceOperations = Map.empty[Instance.Id, Cancellable]

  private[this] var inFlightLaunches = Map.empty[Instance.Id, Launch]

  /** instances that are in flight and those in the tracker */
  private[this] var instanceMap: Map[Instance.Id, Instance] = Map.empty

  private[this] val remainingLaunches: mutable.Queue[Launch] = mutable.Queue.empty

  private[this] var launchingRunSpecs: Map[PathId, (RunSpec, Boolean, Boolean)] = Map.empty

  /** Decorator to use this actor as a [[OfferMatcher#TaskOpSource]] */
  private[this] val myselfAsLaunchSource = InstanceOpSourceDelegate(self)

  case class Launch(runSpecId: PathId, reference: Option[UUID], messageSender: ActorRef)

  override def preStart(): Unit = {
    super.preStart()

    logger.info("Started runSpecLauncherActor")

    // TODO (Keno): how to initialize instanceMap?
    // TODO (Keno): do something about the rate limiter
    // instanceMap = instanceTracker.instancesBySpecSync.instancesMap(runSpec.id).instanceMap
    // rateLimiterActor ! RateLimiterActor.GetDelay(runSpec)
  }

  override def receive: Receive = LoggingReceive.withLabel("active") {
    Seq(
      receiveTaskLaunchNotification,
      receiveInstanceUpdate,
      receiveAddInstance,
      receiveProcessOffers,
      receiveUnknown
    ).reduce(_.orElse[Any, Unit](_))
  }

  private[this] def receiveUnknown: Receive = {
    case msg: Any =>
      // fail fast and do not let the sender time out
      sender() ! Status.Failure(new IllegalStateException(s"Unhandled message: $msg"))
  }

  private[this] def getLaunchByInstanceId(instanceId: Instance.Id): Option[Launch] = {
    val launch = inFlightLaunches.get(instanceId)

    if (launch.isEmpty) {
      logger.warn(s"Could not find launch for $instanceId")
    }

    launch
  }

  private[this] def requeueInstance(instanceId: Instance.Id): Unit = {
    getLaunchByInstanceId(instanceId).foreach(remainingLaunches.enqueue(_))
  }

  private[this] def receiveTaskLaunchNotification: Receive = {
    case InstanceOpSourceDelegate.InstanceOpRejected(op, reason) if inFlight(op) =>
      removeInstance(op.instanceId)
      logger.debug(s"Task op '${op.getClass.getSimpleName}' for ${op.instanceId} was REJECTED, reason '$reason', rescheduling. $status")

      op match {
        // only increment for launch ops, not for reservations:
        case _: InstanceOp.LaunchTask => requeueInstance(op.instanceId)
        case _: InstanceOp.LaunchTaskGroup => requeueInstance(op.instanceId)
        case _ => ()
      }

      OfferMatcherRegistration.manageOfferMatcherStatus()

    case InstanceOpSourceDelegate.InstanceOpRejected(op, RunSpecLauncherActor.OfferOperationRejectedTimeoutReason) =>
      // This is a message that we scheduled in this actor.
      // When we receive a launch confirmation or rejection, we cancel this timer but
      // there is still a race and we might send ourselves the message nevertheless, so we just
      // ignore it here.
      logger.debug(s"Ignoring task launch rejected for '${op.instanceId}' as the task is not in flight anymore")

    case InstanceOpSourceDelegate.InstanceOpRejected(op, reason) =>
      logger.warn(s"Unexpected task op '${op.getClass.getSimpleName}' rejected for ${op.instanceId} with reason $reason")

    case InstanceOpSourceDelegate.InstanceOpAccepted(op) =>
      getLaunchByInstanceId(op.instanceId).foreach(
        launch => launch.messageSender ! RunSpecLauncherActor.InstanceCreated(op.instanceId, launch.reference)
      )
      inFlightInstanceOperations -= op.instanceId
      inFlightLaunches -= op.instanceId
      if (!remainingLaunches.exists(_.runSpecId == op.instanceId.runSpecId)) {
        launchingRunSpecs -= op.instanceId.runSpecId
      }
      logger.debug(s"Task op '${op.getClass.getSimpleName}' for ${op.instanceId} was accepted. $status")
  }

  private[this] def receiveInstanceUpdate: Receive = {
    case change: InstanceChange =>
      change match {
        case update: InstanceUpdated =>
          logger.debug(s"receiveInstanceUpdate: ${update.id} is ${update.condition}")
          instanceMap += update.id -> update.instance

        case update: InstanceDeleted =>
          logger.info(s"receiveInstanceUpdate: ${update.id} was deleted (${update.condition})")
          removeInstance(update.id)
          // A) If the app has constraints, we need to reconsider offers that
          // we already rejected. E.g. when a host:unique constraint prevented
          // us to launch tasks on a particular node before, we need to reconsider offers
          // of that node after a task on that node has died.
          //
          // B) If a reservation timed out, already rejected offers might become eligible for creating new reservations.
          launchingRunSpecs.get(update.id.runSpecId) match {
            case Some((runSpec, _, _)) =>
              if (runSpec.constraints.nonEmpty || (runSpec.residency.isDefined && shouldLaunchInstances)) {
                maybeOfferReviver.foreach (_.reviveOffers ())
              }
            case None => logger.warn("Received instance update for unknown instance")
          }
      }
      sender() ! Done
  }

  private[this] def removeInstance(instanceId: Instance.Id): Unit = {
    inFlightInstanceOperations.get(instanceId).foreach(_.cancel())
    inFlightInstanceOperations -= instanceId
    inFlightLaunches -= instanceId
    instanceMap -= instanceId
  }

  private[this] def receiveAddInstance: Receive = {
    case RunSpecLauncherActor.AddInstance(newRunSpec, restartOnExit, restartOnFailure, uuid) =>
      logger.info(s"queue new instance for for ${newRunSpec.id} with restartOnExit=$restartOnExit and restartOnFailure=$restartOnFailure")
      launchingRunSpecs += newRunSpec.id -> ((newRunSpec, restartOnExit, restartOnFailure))
      remainingLaunches.enqueue(Launch(newRunSpec.id, uuid, sender()))

      OfferMatcherRegistration.manageOfferMatcherStatus()
  }

  private[this] def receiveProcessOffers: Receive = {
    case ActorOfferMatcher.MatchOffer(offer, promise) if !shouldLaunchInstances =>
      logger.debug(s"Ignoring offer ${offer.getId.getValue}: $status")
      promise.trySuccess(MatchedInstanceOps.noMatch(offer.getId))

    case ActorOfferMatcher.MatchOffer(offer, promise) =>
      val reachableInstances = instanceMap.filterNotAs{ case (_, instance) => instance.state.condition.isLost }

      val maybeMatched = remainingLaunches.foldLeft[Option[OfferMatchResult.Match]](None)((maybeMatch, launch) => {
        maybeMatch.orElse(
          launchingRunSpecs.get(launch.runSpecId) match {
            case Some((runSpec, restartOnExit, restartOnFailure)) =>
              val matchRequest = InstanceOpFactory.Request(runSpec, restartOnExit, restartOnFailure, offer, reachableInstances, additionalLaunches = 1)
              instanceOpFactory.matchOfferRequest(matchRequest) match {
                case matched: OfferMatchResult.Match =>
                  inFlightLaunches += matched.instanceOp.instanceId -> launch
                  Some(matched)
                case notMatched: OfferMatchResult.NoMatch =>
                  None
              }
            case None =>
              logger.warn("Found non-existent runSpecId in queue while processing offer")
              None
          }
        )
      })

      maybeMatched match {
        case Some(matched) => handleInstanceOp(matched.instanceOp, offer, promise)
        case None => promise.trySuccess(MatchedInstanceOps.noMatch(offer.getId))
      }
  }

  /**
    * Mutate internal state in response to having matched an instanceOp.
    *
    * @param instanceOp The instanceOp that is to be applied to on a previously
    *     received offer
    * @param offer The offer that could be matched successfully.
    * @param promise Promise that tells offer matcher that the offer has been accepted.
    */
  private[this] def handleInstanceOp(instanceOp: InstanceOp, offer: Mesos.Offer, promise: Promise[MatchedInstanceOps]): Unit = {
    def updateActorState(): Unit = {
      val instanceId = instanceOp.instanceId
      instanceOp match {
        // only decrement for launched instances, not for reservations:
        case _: InstanceOp.LaunchTask => remainingLaunches.dequeueFirst(_.runSpecId == instanceId.runSpecId)
        case _: InstanceOp.LaunchTaskGroup => remainingLaunches.dequeueFirst(_.runSpecId == instanceId.runSpecId)
        case _ => ()
      }

      // We will receive the updated instance once it's been persisted. Before that,
      // we can only store the possible state, as we don't have the updated state
      // yet.
      instanceOp.stateOp.possibleNewState.foreach { newState =>
        instanceMap += instanceId -> newState
        // In case we don't receive an update a TaskOpRejected message with TASK_OP_REJECTED_TIMEOUT_REASON
        // reason is scheduled within config.taskOpNotificationTimeout milliseconds. This will trigger another
        // attempt to launch the task.
        //
        // NOTE: this can lead to a race condition where an additional task is launched: in a nutshell if a TaskOp A
        // is rejected due to an internal timeout, TaskLauncherActor will schedule another TaskOp.Launch B, because
        // it's under the impression that A has not succeeded/got lost. If the timeout is triggered internally, the
        // tasksToLaunch count will be increased by 1. The TaskOp A can still be accepted though, and if it is,
        // two tasks (A and B) might be launched instead of one (given Marathon receives sufficient offers and is
        // able to create another TaskOp).
        // This would lead to the app being over-provisioned (e.g. "3 of 2 tasks" launched) but eventually converge
        // to the target task count when tasks over capacity are killed. With a sufficiently high timeout this case
        // should be fairly rare.
        // A better solution would involve an overhaul of the way TaskLaunchActor works and might be
        // a subject to change in the future.
        scheduleTaskOpTimeout(instanceOp)
      }

      OfferMatcherRegistration.manageOfferMatcherStatus()
    }

    launchingRunSpecs.get(instanceOp.instanceId.runSpecId) match {
      case Some((runSpec, _, _)) =>
        updateActorState()
        logger.debug(s"Request ${instanceOp.getClass.getSimpleName} for instance '${instanceOp.instanceId.idString}', version '${runSpec.version}'. $status")
        promise.trySuccess(MatchedInstanceOps(offer.getId, Seq(InstanceOpWithSource(myselfAsLaunchSource, instanceOp))))
      case None =>
        logger.warn("Received handleInstanceOp for unknown run spec")
        promise.trySuccess(MatchedInstanceOps.noMatch(offer.getId))
    }
  }

  private[this] def scheduleTaskOpTimeout(instanceOp: InstanceOp): Unit = {
    val reject = InstanceOpSourceDelegate.InstanceOpRejected(
      instanceOp, RunSpecLauncherActor.OfferOperationRejectedTimeoutReason
    )
    val cancellable = scheduleTaskOperationTimeout(context, reject)
    inFlightInstanceOperations += instanceOp.instanceId -> cancellable
  }

  private[this] def inFlight(op: InstanceOp): Boolean = inFlightInstanceOperations.contains(op.instanceId)

  protected def scheduleTaskOperationTimeout(
    context: ActorContext,
    message: InstanceOpSourceDelegate.InstanceOpRejected): Cancellable =
    {
      import context.dispatcher
      context.system.scheduler.scheduleOnce(DurationInt(30000).milliseconds, self, message)
    }

  private[this] def shouldLaunchInstances: Boolean = remainingLaunches.nonEmpty

  private[this] def status: String = {
    val inFlight = inFlightInstanceOperations.size
    val launchedOrRunning = instanceMap.values.count(_.isLaunched) - inFlight
    s"${remainingLaunches.size} run specs to launch, $inFlight in flight, " +
      s"$launchedOrRunning confirmed"
  }

  /** Manage registering this actor as offer matcher. Only register it if instancesToLaunch > 0. */
  private[this] object OfferMatcherRegistration {
    private[this] val myselfAsOfferMatcher: OfferMatcher = {
      new ActorOfferMatcher(self, None)(context.system.scheduler)
    }
    private[this] var registeredAsMatcher = false

    /** Register/unregister as necessary */
    def manageOfferMatcherStatus(): Unit = {
      val shouldBeRegistered = shouldLaunchInstances

      if (shouldBeRegistered && !registeredAsMatcher) {
        logger.debug("Registering deployment offer matcher")
        offerMatcherManager.addSubscription(myselfAsOfferMatcher)(context.dispatcher)
        registeredAsMatcher = true
      } else if (!shouldBeRegistered && registeredAsMatcher) {
        logger.info("No tasks left to launch. Stop receiving offers deployment offer matcher")
        offerMatcherManager.removeSubscription(myselfAsOfferMatcher)(context.dispatcher)
        registeredAsMatcher = false
      }
    }

    def unregister(): Unit = {
      if (registeredAsMatcher) {
        logger.info("Deregister as matcher.")
        offerMatcherManager.removeSubscription(myselfAsOfferMatcher)(context.dispatcher)
        registeredAsMatcher = false
      }
    }
  }
}
