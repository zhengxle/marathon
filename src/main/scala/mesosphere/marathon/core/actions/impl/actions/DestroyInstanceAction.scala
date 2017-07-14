package mesosphere.marathon
package core.actions.impl.actions

import java.util.UUID

import mesosphere.marathon.core.actions.{ ActionStatus, InstanceAction }
import mesosphere.marathon.core.condition.Condition
import mesosphere.marathon.core.instance.Instance
import mesosphere.marathon.core.instance.update.InstanceChange
import mesosphere.marathon.core.task.termination.KillReason

case class DestroyInstanceAction(instanceId: Instance.Id, killReason: KillReason, uuid: UUID) extends InstanceAction {
  override var affectedInstanceId: Option[Instance.Id] = Some(instanceId)

  override def getStatusForInstanceChange(change: InstanceChange): ActionStatus.Value = {
    change.condition match {
      case Condition.Killed => ActionStatus.Succeeded
      case _ => ActionStatus.Running
    }
  }
}
