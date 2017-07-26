package mesosphere.marathon
package core.flow

import mesosphere.marathon.core.flow.impl.OfferMatcherLaunchTokensActor
import mesosphere.marathon.core.leadership.LeadershipModule
import mesosphere.marathon.core.matcher.manager.OfferMatcherManager
import mesosphere.marathon.core.task.bus.TaskChangeObservables
import org.slf4j.LoggerFactory

/**
  * This module contains code for managing the flow/backpressure of the application.
  */
class FlowModule(leadershipModule: LeadershipModule) {
  private[this] val log = LoggerFactory.getLogger(getClass)

  /**
    * Refills the launch tokens of the OfferMatcherManager periodically. See [[LaunchTokenConfig]] for configuration.
    *
    * Also adds a launch token to the OfferMatcherManager for every update we get about a new running tasks.
    *
    * The reasoning is that getting infos about running tasks signals that the Mesos infrastructure is working
    * and not yet completely overloaded.
    */
  def refillOfferMatcherManagerLaunchTokens(
    conf: LaunchTokenConfig,
    taskStatusObservables: TaskChangeObservables,
    offerMatcherManager: OfferMatcherManager): Unit =
    {
      lazy val offerMatcherLaunchTokensProps = OfferMatcherLaunchTokensActor.props(
        conf, taskStatusObservables, offerMatcherManager
      )
      leadershipModule.startWhenLeader(offerMatcherLaunchTokensProps, "offerMatcherLaunchTokens")

    }
}
