package mesosphere.marathon
package core.launchqueue.impl

import java.time.Clock

import akka.Done
import akka.actor._
import akka.event.LoggingReceive
import com.typesafe.scalalogging.StrictLogging
import mesosphere.marathon.core.condition.Condition
import mesosphere.marathon.core.flow.OfferReviver
import mesosphere.marathon.core.instance.Instance
import mesosphere.marathon.core.instance.update.{ InstanceChange, InstanceDeleted, InstanceUpdateOperation, InstanceUpdated }
import mesosphere.marathon.core.launcher.{ InstanceOp, InstanceOpFactory, OfferMatchResult }
import mesosphere.marathon.core.launchqueue.LaunchQueue.QueuedInstanceInfo
import mesosphere.marathon.core.launchqueue.LaunchQueueConfig
import mesosphere.marathon.core.launchqueue.impl.TaskLauncherActor.RecheckIfBackOffUntilReached
import mesosphere.marathon.core.matcher.base.OfferMatcher
import mesosphere.marathon.core.matcher.base.OfferMatcher.{ InstanceOpWithSource, MatchedInstanceOps }
import mesosphere.marathon.core.matcher.base.util.{ ActorOfferMatcher, InstanceOpSourceDelegate }
import mesosphere.marathon.core.matcher.manager.OfferMatcherManager
import mesosphere.marathon.core.task.Task
import mesosphere.marathon.core.task.tracker.{ InstanceStateOpProcessor, InstanceTracker }
import mesosphere.marathon.state.{ PathId, Region, RunSpec, Timestamp }
import mesosphere.marathon.stream.Implicits._
import org.apache.mesos.{ Protos => Mesos }

import scala.collection.mutable
import scala.concurrent.Promise
import scala.concurrent.duration._

case class InstanceToLaunch(runSpec: RunSpec, condition: Condition = Condition.Scheduled)

private[launchqueue] object TaskLauncherActor {
  def props(
    config: LaunchQueueConfig,
    offerMatcherManager: OfferMatcherManager,
    clock: Clock,
    taskOpFactory: InstanceOpFactory,
    maybeOfferReviver: Option[OfferReviver],
    instanceTracker: InstanceTracker,
    rateLimiterActor: ActorRef,
    offerMatchStatisticsActor: ActorRef,
    localRegion: () => Option[Region])(
    runSpecId: PathId,
    initialInstances: mutable.Map[Instance.Id, InstanceToLaunch]): Props = {
    Props(new TaskLauncherActor(
      config,
      offerMatcherManager,
      clock, taskOpFactory,
      maybeOfferReviver,
      instanceTracker, rateLimiterActor, offerMatchStatisticsActor,
      runSpecId, initialInstances, localRegion))
  }

  sealed trait Requests

  /**
    * Increase the instance count of the receiver.
    * The actor responds with a [[QueuedInstanceInfo]] message.
    */
  case class AddInstances(spec: RunSpec, count: Int) extends Requests
  /**
    * Get the current count.
    * The actor responds with a [[QueuedInstanceInfo]] message.
    */
  case object GetCount extends Requests

  /**
    * Results in rechecking whether we may launch tasks.
    */
  private case object RecheckIfBackOffUntilReached extends Requests

  case object Stop extends Requests

  private val OfferOperationRejectedTimeoutReason: String =
    "InstanceLauncherActor: no accept received within timeout. " +
      "You can reconfigure the timeout with --task_operation_notification_timeout."
}

/**
  * Allows processing offers for starting tasks for the given app.
  */
private class TaskLauncherActor(
    config: LaunchQueueConfig,
    offerMatcherManager: OfferMatcherManager,
    clock: Clock,
    instanceOpFactory: InstanceOpFactory,
    maybeOfferReviver: Option[OfferReviver],
    instanceTracker: InstanceTracker,
    //    stateOpProcessor: InstanceStateOpProcessor,
    rateLimiterActor: ActorRef,
    offerMatchStatisticsActor: ActorRef,
    val runSpecId: PathId,
    private[this] val instancesToLaunch: mutable.Map[Instance.Id, InstanceToLaunch] = mutable.Map.empty, // TODO: These should be saved in the instance tracker as scheduled
    localRegion: () => Option[Region]) extends Actor with StrictLogging with Stash {
  // scalastyle:on parameter.number

  private[this] var inFlightInstanceOperations = Map.empty[Instance.Id, Cancellable]

  private[this] var recheckBackOff: Option[Cancellable] = None
  private[this] var backOffUntil: Option[Timestamp] = None

  /** instances that are in flight and those in the tracker */
  private[this] var instanceMap: Map[Instance.Id, Instance] = _

  /** Decorator to use this actor as a [[OfferMatcher#TaskOpSource]] */
  private[this] val myselfAsLaunchSource = InstanceOpSourceDelegate(self)

  private[this] val startedAt = clock.now()

  override def preStart(): Unit = {
    super.preStart()

    logger.info(s"Started instanceLaunchActor for ${runSpecId} with initial $instancesToLaunch")

    // TODO: We want to save scheudled instances in the instance tracker instead of the task launcher.
    // Create Scheduled instances
    //    instancesToLaunch.iterator.foreach { case (instanceId, InstanceToLaunch(runSpec, _)) =>
    //      val scheduleOp = InstanceUpdateOperation.Schedule(instanceId, runSpec)
    //      stateOpProcessor.process(scheduleOp)
    //    }

    instanceMap = instanceTracker.instancesBySpecSync.instancesMap(runSpecId).instanceMap
    rateLimiterActor ! RateLimiterActor.GetDelay(instancesToLaunch.values.map(_.runSpec).head)
  }

  override def postStop(): Unit = {
    OfferMatcherRegistration.unregister()
    recheckBackOff.foreach(_.cancel())

    if (inFlightInstanceOperations.nonEmpty) {
      logger.warn(s"Actor shutdown while instances are in flight: ${inFlightInstanceOperations.keys.mkString(", ")}")
      inFlightInstanceOperations.values.foreach(_.cancel())
    }

    offerMatchStatisticsActor ! OfferMatchStatisticsActor.LaunchFinished(runSpecId)

    super.postStop()

    logger.info(s"Stopped InstanceLauncherActor for ${runSpecId}")
  }

  override def receive: Receive = waitForInitialDelay

  private[this] def waitForInitialDelay: Receive = LoggingReceive.withLabel("waitingForInitialDelay") {
    case RateLimiterActor.DelayUpdate(spec, delayUntil) if spec.id == runSpecId =>
      stash()
      unstashAll()
      context.become(active)
    case msg @ RateLimiterActor.DelayUpdate(spec, delayUntil) if spec.id != runSpecId =>
      logger.warn(s"Received delay update for other runSpec: $msg")
    case message: Any => stash()
  }

  private[this] def active: Receive = LoggingReceive.withLabel("active") {
    Seq(
      receiveStop,
      receiveDelayUpdate,
      receiveTaskLaunchNotification,
      receiveInstanceUpdate,
      receiveGetCurrentCount,
      receiveAddCount,
      receiveProcessOffers,
      receiveUnknown
    ).reduce(_.orElse[Any, Unit](_))
  }

  private[this] def receiveUnknown: Receive = {
    case msg: Any =>
      // fail fast and do not let the sender time out
      sender() ! Status.Failure(new IllegalStateException(s"Unhandled message: $msg"))
  }

  private[this] def receiveStop: Receive = {
    case TaskLauncherActor.Stop =>
      if (inFlightInstanceOperations.nonEmpty) {
        val taskIds = inFlightInstanceOperations.keys.take(3).mkString(", ")
        logger.info(
          s"Still waiting for ${inFlightInstanceOperations.size} inflight messages but stopping anyway. " +
            s"First three task ids: $taskIds"
        )
      }
      context.stop(self)
  }

  /**
    * Receive rate limiter updates.
    */
  private[this] def receiveDelayUpdate: Receive = {
    case RateLimiterActor.DelayUpdate(spec, delayUntil) if spec.id == runSpecId =>

      if (!backOffUntil.contains(delayUntil)) {

        backOffUntil = Some(delayUntil)

        recheckBackOff.foreach(_.cancel())
        recheckBackOff = None

        val now: Timestamp = clock.now()
        if (backOffUntil.exists(_ > now)) {
          import context.dispatcher
          recheckBackOff = Some(
            context.system.scheduler.scheduleOnce(now until delayUntil, self, RecheckIfBackOffUntilReached)
          )
        }

        OfferMatcherRegistration.manageOfferMatcherStatus()
      }

      logger.debug(s"After delay update $status")

    case msg @ RateLimiterActor.DelayUpdate(spec, delayUntil) if spec.id != runSpecId =>
      logger.warn(s"Received delay update for other runSpec: $msg")

    case RecheckIfBackOffUntilReached => OfferMatcherRegistration.manageOfferMatcherStatus()
  }

  private[this] def receiveTaskLaunchNotification: Receive = {
    case InstanceOpSourceDelegate.InstanceOpRejected(op, reason) if inFlight(op) =>
      removeInstance(op.instanceId)
      logger.debug(s"Task op '${op.getClass.getSimpleName}' for ${op.instanceId} was REJECTED, reason '$reason', rescheduling. $status")

      op match {
        // only increment for launch ops, not for reservations:
        case InstanceOp.LaunchTask(_, _, _, _, runSpec) => instancesToLaunch += Instance.Id.forRunSpec(runSpecId) -> InstanceToLaunch(runSpec)
        case InstanceOp.LaunchTaskGroup(_, _, _, _, _, runSpec) => instancesToLaunch += Instance.Id.forRunSpec(runSpecId) -> InstanceToLaunch(runSpec)
        //case _: InstanceOp.LaunchTask => instancesToLaunch += op.instanceId -> InstanceToLaunch(runSpec)
        //case _: InstanceOp.LaunchTaskGroup => instancesToLaunch += op.instanceId -> InstanceToLaunch(runSpec)
        case _ => ()
      }

      OfferMatcherRegistration.manageOfferMatcherStatus()

    case InstanceOpSourceDelegate.InstanceOpRejected(op, TaskLauncherActor.OfferOperationRejectedTimeoutReason) =>
      // This is a message that we scheduled in this actor.
      // When we receive a launch confirmation or rejection, we cancel this timer but
      // there is still a race and we might send ourselves the message nevertheless, so we just
      // ignore it here.
      logger.debug(s"Ignoring task launch rejected for '${op.instanceId}' as the task is not in flight anymore")

    case InstanceOpSourceDelegate.InstanceOpRejected(op, reason) =>
      logger.warn(s"Unexpected task op '${op.getClass.getSimpleName}' rejected for ${op.instanceId} with reason $reason")

    case InstanceOpSourceDelegate.InstanceOpAccepted(op) =>
      inFlightInstanceOperations -= op.instanceId
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
          val toLaunch = instancesToLaunch(update.id)
          instancesToLaunch -= update.id
          // A) If the app has constraints, we need to reconsider offers that
          // we already rejected. E.g. when a host:unique constraint prevented
          // us to launch tasks on a particular node before, we need to reconsider offers
          // of that node after a task on that node has died.
          //
          // B) If a reservation timed out, already rejected offers might become eligible for creating new reservations.
          if (toLaunch.runSpec.constraints.nonEmpty || (toLaunch.runSpec.isResident && shouldLaunchInstances)) {
            maybeOfferReviver.foreach(_.reviveOffers())
          }
      }
      sender() ! Done
  }

  private[this] def removeInstance(instanceId: Instance.Id): Unit = {
    inFlightInstanceOperations.get(instanceId).foreach(_.cancel())
    inFlightInstanceOperations -= instanceId
    instanceMap -= instanceId
  }

  private[this] def receiveGetCurrentCount: Receive = {
    case TaskLauncherActor.GetCount =>
      replyWithQueuedInstanceCount()
  }

  private[this] def receiveAddCount: Receive = {
    case TaskLauncherActor.AddInstances(newRunSpec, addCount) =>
      logger.debug(s"Received add instances for ${newRunSpec.id}, version ${newRunSpec.version} with count $addCount.")

      instancesToLaunch ++= 1.to(addCount).map { _ =>
        Instance.Id.forRunSpec(newRunSpec.id) -> InstanceToLaunch(newRunSpec)
      }

      val configChange = instancesToLaunch.exists { case (_, InstanceToLaunch(runSpec, _)) => runSpec.isUpgrade(newRunSpec) }
      //???? val needsRestart = instancesToLaunch.exists { case (_, InstanceToLaunch(runSpec)) => runSpec.needsRestart(newRunSpec) }
      if (configChange) {
        logger.info(s"getting new runSpec for '${runSpecId}', version ${newRunSpec.version} with $addCount initial instances")

        suspendMatchingUntilWeGetBackoffDelayUpdate()
      }

      OfferMatcherRegistration.manageOfferMatcherStatus()

      replyWithQueuedInstanceCount()
  }

  private[this] def suspendMatchingUntilWeGetBackoffDelayUpdate(): Unit = {
    // signal no interest in new offers until we get the back off delay.
    // this makes sure that we see unused offers again that we rejected for the old configuration.
    OfferMatcherRegistration.unregister()

    // get new back off delay, don't do anything until we get that.
    backOffUntil = None
    rateLimiterActor ! RateLimiterActor.GetDelay(instancesToLaunch.head._2.runSpec)
    context.become(waitForInitialDelay)
  }

  private[this] def replyWithQueuedInstanceCount(): Unit = {
    val instancesLaunched = instanceMap.values.count(instance => instance.isLaunched || instance.isReserved)
    val instancesLaunchesInFlight = inFlightInstanceOperations.keys
      .count(instanceId => instanceMap.get(instanceId).exists(instance => instance.isLaunched || instance.isReserved))
    sender() ! QueuedInstanceInfo(
      instancesToLaunch.head._2.runSpec,
      inProgress = instancesToLaunch.size > 0 || inFlightInstanceOperations.nonEmpty,
      instancesLeftToLaunch = instancesToLaunch.size,
      finalInstanceCount = instancesToLaunch.size + instancesLaunchesInFlight + instancesLaunched,
      backOffUntil.getOrElse(clock.now()),
      startedAt
    )
  }

  private[this] def receiveProcessOffers: Receive = {
    case ActorOfferMatcher.MatchOffer(offer, promise) if !shouldLaunchInstances =>
      logger.debug(s"Ignoring offer ${offer.getId.getValue}: $status")
      promise.trySuccess(MatchedInstanceOps.noMatch(offer.getId))

    case ActorOfferMatcher.MatchOffer(offer, promise) =>
      logger.debug(s"Matching offer ${offer.getId} and need to launch $instancesToLaunch tasks.")
      val reachableInstances = instanceMap.filterNotAs{ case (_, instance) => instance.state.condition.isLost }

      val (instanceId, runSpec) = instancesToLaunch.collectFirst { case (id, InstanceToLaunch(spec, condition)) if condition.isScheduled => (id, spec) }.get
      val matchRequest = InstanceOpFactory.Request(runSpec, offer, reachableInstances, instanceId, localRegion())
      instanceOpFactory.matchOfferRequest(matchRequest) match {
        case matched: OfferMatchResult.Match =>
          logger.debug(s"Matched offer ${offer.getId} for run spec ${runSpec.id}, ${runSpec.version}.")
          offerMatchStatisticsActor ! matched
          handleInstanceOp(matched.instanceOp, offer, promise)
        case notMatched: OfferMatchResult.NoMatch =>
          logger.debug(s"Did not match offer ${offer.getId} for run spec ${runSpec.id}, ${runSpec.version}.")
          offerMatchStatisticsActor ! notMatched
          promise.trySuccess(MatchedInstanceOps.noMatch(offer.getId))
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
        case task: InstanceOp.LaunchTask =>
          // Update instance ID
          val toLaunch = instancesToLaunch(instanceId)
          instancesToLaunch += instanceId -> toLaunch.copy(condition = Condition.Created)
        case tasks: InstanceOp.LaunchTaskGroup =>
          // Update instance ID
          val toLaunch = instancesToLaunch(instanceId)
          instancesToLaunch += instanceId -> toLaunch.copy(condition = Condition.Created)
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

    updateActorState()

    val runSpec = instancesToLaunch(instanceOp.instanceId).runSpec

    logger.debug(s"Request ${instanceOp.getClass.getSimpleName} for instance '${instanceOp.instanceId.idString}', version '${runSpec.version}'. $status")
    promise.trySuccess(MatchedInstanceOps(offer.getId, Seq(InstanceOpWithSource(myselfAsLaunchSource, instanceOp))))
  }

  private[this] def scheduleTaskOpTimeout(instanceOp: InstanceOp): Unit = {
    val reject = InstanceOpSourceDelegate.InstanceOpRejected(
      instanceOp, TaskLauncherActor.OfferOperationRejectedTimeoutReason
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
      context.system.scheduler.scheduleOnce(config.taskOpNotificationTimeout().milliseconds, self, message)
    }

  private[this] def backoffActive: Boolean = backOffUntil.forall(_ > clock.now())
  private[this] def shouldLaunchInstances: Boolean = instancesToLaunch.size > 0 && !backoffActive

  private[this] def status: String = {
    val backoffStr = backOffUntil match {
      case Some(until) if until > clock.now() => s"currently waiting for backoff($until)"
      case _ => "not backing off"
    }

    val inFlight = inFlightInstanceOperations.size
    val launchedOrRunning = instanceMap.values.count(_.isLaunched) - inFlight
    val matchInstanceStr = s"${instancesToLaunch.size} instancesToLaunch, $inFlight in flight, "

    s"$launchedOrRunning confirmed. $matchInstanceStr $backoffStr"
  }

  /** Manage registering this actor as offer matcher. Only register it if instancesToLaunch > 0. */
  private[this] object OfferMatcherRegistration {
    private[this] val myselfAsOfferMatcher: OfferMatcher = {
      //set the precedence only, if this app is resident
      val maybeResidentRunSpec = instancesToLaunch.collectFirst {
        case (_, InstanceToLaunch(runSpec, condition)) if condition.isScheduled => runSpec
      }
      new ActorOfferMatcher(self, maybeResidentRunSpec.map(_.id))
    }
    private[this] var registeredAsMatcher = false

    /** Register/unregister as necessary */
    def manageOfferMatcherStatus(): Unit = {
      val shouldBeRegistered = shouldLaunchInstances

      if (shouldBeRegistered && !registeredAsMatcher) {
        logger.debug(s"Registering for ${runSpecId}.")
        offerMatcherManager.addSubscription(myselfAsOfferMatcher)(context.dispatcher)
        registeredAsMatcher = true
      } else if (!shouldBeRegistered && registeredAsMatcher) {
        if (instancesToLaunch.size > 0) {
          logger.info(s"Backing off due to task failures. Stop receiving offers for ${runSpecId}, versions ${instancesToLaunch.values.map(_.runSpec.version).toSet}")
        } else {
          logger.info(s"No tasks left to launch. Stop receiving offers for ${runSpecId}")
        }
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
