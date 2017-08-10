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
    val restart = update.condition match {
      case Condition.Failed => true
      case Condition.Finished => true
      case _ => false
    }

    if (restart) {
      val runSpecId = update.instance.runSpecId
      logger.info(s"Restarting run spec $runSpecId after instance ${update.instance.instanceId} stopped unexpectedly")
      actionManager.add(CreateInstanceAction(runSpecId, UUID.randomUUID()))
    }

    Future.successful(Done)
  }
}
