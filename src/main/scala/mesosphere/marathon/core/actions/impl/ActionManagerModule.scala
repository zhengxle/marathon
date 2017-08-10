package mesosphere.marathon
package core.actions.impl

import akka.actor.ActorRef
import mesosphere.marathon.core.actions.ActionManager
import mesosphere.marathon.core.base.Clock
import mesosphere.marathon.core.flow.OfferReviver
import mesosphere.marathon.core.group.GroupManager
import mesosphere.marathon.core.launcher.InstanceOpFactory
import mesosphere.marathon.core.leadership.LeadershipModule
import mesosphere.marathon.core.matcher.manager.OfferMatcherManager
import mesosphere.marathon.core.task.termination.KillService
import mesosphere.marathon.core.task.tracker.InstanceTracker

import scala.concurrent.ExecutionContext

/**
  * Provides an [[ActionManagerActor]] implementation
  */
class ActionManagerModule(
    groupManager: GroupManager,
    leadershipModule: LeadershipModule,
    clock: Clock,
    subOfferMatcherManager: OfferMatcherManager,
    maybeOfferReviver: Option[OfferReviver],
    taskTracker: InstanceTracker,
    taskOpFactory: InstanceOpFactory,
    killService: KillService)(implicit ec: ExecutionContext) {

  private[this] val actionManagerActorRef: ActorRef = {
    val props = ActionManagerActor.props(
      groupManager,
      taskTracker,
      subOfferMatcherManager,
      clock,
      taskOpFactory,
      maybeOfferReviver,
      killService)
    leadershipModule.startWhenLeader(props, "actionManager")
  }

  val actionManager: ActionManager = new ActionManagerDelegate(actionManagerActorRef)
}
