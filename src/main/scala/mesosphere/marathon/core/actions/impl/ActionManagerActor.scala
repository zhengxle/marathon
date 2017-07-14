package mesosphere.marathon
package core.actions.impl

import java.util.UUID

import akka.actor.{ Actor, ActorLogging, ActorRef, Props }
import mesosphere.marathon.core.actions.impl.ActionManagerActor.{ ActionHandlerResult, AddAction, Result }
import mesosphere.marathon.core.actions.{ Action, ActionStatus, InstanceAction }
import mesosphere.marathon.core.actions.impl.ImmediateDispatcherActor.DestroyInstance
import mesosphere.marathon.core.actions.impl.RunSpecLauncherActor.AddInstance
import mesosphere.marathon.core.actions.impl.actions.{ CreateInstanceAction, DestroyInstanceAction }
import mesosphere.marathon.core.base.Clock
import mesosphere.marathon.core.flow.OfferReviver
import mesosphere.marathon.core.group.GroupManager
import mesosphere.marathon.core.instance.Instance
import mesosphere.marathon.core.instance.update.InstanceChange
import mesosphere.marathon.core.launcher.InstanceOpFactory
import mesosphere.marathon.core.launchqueue.LaunchQueueConfig
import mesosphere.marathon.core.matcher.manager.OfferMatcherManager
import mesosphere.marathon.core.task.termination.KillService
import mesosphere.marathon.core.task.tracker.InstanceTracker
import mesosphere.marathon.state.PathId

import scala.collection.mutable
import scala.concurrent.{ ExecutionContext, Future }

class ActionManagerActor(
    groupManager: GroupManager,
    instanceTracker: InstanceTracker,
    config: LaunchQueueConfig,
    offerMatcherManager: OfferMatcherManager,
    clock: Clock,
    taskOpFactory: InstanceOpFactory,
    maybeOfferReviver: Option[OfferReviver],
    rateLimiterActor: ActorRef,
    offerMatchStatisticsActor: ActorRef,
    killService: KillService)(implicit ec: ExecutionContext) extends Actor with ActorLogging {
  val runSpecLauncherActor: ActorRef = context.actorOf(RunSpecLauncherActor.props(
    config,
    offerMatcherManager,
    clock,
    taskOpFactory,
    maybeOfferReviver,
    instanceTracker,
    rateLimiterActor,
    offerMatchStatisticsActor
  ), name = "offermatcher")
  val immediateDispatcherActor: ActorRef = context.actorOf(ImmediateDispatcherActor.props(killService), name = "dispatcher")

  val runningActions: mutable.Queue[Action] = mutable.Queue.empty
  var instanceActionMap: Map[Instance.Id, InstanceAction] = _

  override def receive: Receive = {
    case AddAction(action) => addAction(action).foreach(sender ! _)
    case change: InstanceChange => receiveInstanceChange(change)
    case RunSpecLauncherActor.InstanceCreated(instanceId, actionId) => trackInstanceCreation(instanceId, actionId)
  }

  private[this] def trackInstanceCreation(instanceId: Instance.Id, maybeUuid: Option[UUID]): Unit = {
    log.info(s"Instance $instanceId created")

    if (maybeUuid.isEmpty) {
      log.warning("UUID not set when it should have been.")
    }

    maybeUuid.foreach(uuid => runningActions.find(_.uuid == uuid) match {
      case Some(action: InstanceAction) => instanceActionMap += instanceId -> action
      case Some(_) => log.warning("Action obtained from instance creation is not an instance action")
      case None => log.warning(s"No action found for UUID $uuid")
    })
  }

  // Update status of instance actions as instance updates come in
  private[this] def receiveInstanceChange(instanceChange: InstanceChange) = {
    instanceActionMap.get(instanceChange.id).foreach(instanceAction => {
      instanceAction.updateStatusForInstanceChange(instanceChange)
      // TODO (Keno): Persist updated action
    })
  }

  def addAction(action: Action): Future[ActionHandlerResult] = {
    runningActions.enqueue(action)

    action match {
      case action: CreateInstanceAction => handleCreate(action)
      case action: DestroyInstanceAction => handleDestroy(action)
      case _ => Future.successful(Result.UnknownAction())
    }
  }

  def handleCreate(action: CreateInstanceAction): Future[ActionHandlerResult] = {
    groupManager.runSpec(action.runSpecId) match {
      case Some(runSpec) =>
        action.setStatus(ActionStatus.Running)
        // TODO (Keno): Persist updated action
        runSpecLauncherActor ! AddInstance(runSpec, Some(action.uuid))
        Future.successful(Result.Success())
      case None =>
        Future.successful(Result.RunSpecNotFound(action.runSpecId))
    }
  }

  def handleDestroy(action: DestroyInstanceAction): Future[ActionHandlerResult] = {
    instanceTracker.instance(action.instanceId).map({
      case Some(instance) =>
        action.setStatus(ActionStatus.Running)
        // TODO (Keno): Persist updated action
        instanceActionMap += action.instanceId -> action
        immediateDispatcherActor ! DestroyInstance(instance, action.killReason)
        Result.Success()
      case None =>
        Result.InstanceNotFound(action.instanceId)
    })
  }
}

object ActionManagerActor {
  def props(
    groupManager: GroupManager,
    instanceTracker: InstanceTracker,
    config: LaunchQueueConfig,
    offerMatcherManager: OfferMatcherManager,
    clock: Clock,
    taskOpFactory: InstanceOpFactory,
    maybeOfferReviver: Option[OfferReviver],
    rateLimiterActor: ActorRef,
    offerMatchStatisticsActor: ActorRef,
    killService: KillService
  )(implicit ec: ExecutionContext): Props = Props(new ActionManagerActor(
    groupManager,
    instanceTracker,
    config,
    offerMatcherManager,
    clock,
    taskOpFactory,
    maybeOfferReviver,
    rateLimiterActor,
    offerMatchStatisticsActor,
    killService
  ))

  case class AddAction(action: Action)

  trait ActionHandlerResult {}
  object Result {
    case class Success() extends ActionHandlerResult
    case class RunSpecNotFound(runSpecId: PathId) extends ActionHandlerResult
    case class InstanceNotFound(instanceId: Instance.Id) extends ActionHandlerResult
    case class UnknownAction() extends ActionHandlerResult
  }
}