package mesosphere.marathon
package core.task.update.impl.steps

import java.util.UUID

import akka.Done
import com.google.inject.{ Inject, Provider }
import com.typesafe.scalalogging.StrictLogging
import mesosphere.marathon.core.actions.ActionManager
import mesosphere.marathon.core.actions.impl.actions.CreateInstanceAction
import mesosphere.marathon.core.condition.Condition
import mesosphere.marathon.core.instance.update.{ InstanceChange, InstanceChangeHandler }

import scala.concurrent.Future

/**
  * Notify the health check manager of this update.
  */
class RestartRunSpecStepImpl @Inject() (actionManagerProvider: Provider[ActionManager]) extends InstanceChangeHandler with StrictLogging {
  private[this] lazy val actionManager = actionManagerProvider.get()

  override def name: String = "restartRunSpec"

  override def process(update: InstanceChange): Future[Done] = {
    val instanceId = update.instance.instanceId
    val restartOnExit = update.instance.restartOnExit
    val restartOnFailure = update.instance.restartOnFailure
    logger.info(s"Instance $instanceId (restartOnExit=$restartOnExit, restartOnFailure=$restartOnFailure) failed. Checking restart")

    val conditionPermitsRestart = update.condition match {
      case Condition.Failed if update.instance.restartOnFailure => true
      case Condition.Finished if update.instance.restartOnExit => true
      case _ => false
    }

    if (conditionPermitsRestart) {
      val runSpecId = update.instance.runSpecId
      logger.info(s"Restarting run spec $runSpecId after instance $instanceId stopped unexpectedly")
      actionManager.add(CreateInstanceAction(runSpecId, update.instance.restartOnExit, update.instance.restartOnFailure, UUID.randomUUID()))
    }

    Future.successful(Done)
  }
}
