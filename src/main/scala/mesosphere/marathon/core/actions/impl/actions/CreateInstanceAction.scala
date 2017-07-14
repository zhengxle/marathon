package mesosphere.marathon
package core.actions.impl.actions

import java.util.UUID

import mesosphere.marathon.core.actions.{ ActionStatus, InstanceAction }
import mesosphere.marathon.core.condition.Condition
import mesosphere.marathon.core.instance.Instance
import mesosphere.marathon.core.instance.update.InstanceChange
import mesosphere.marathon.state.PathId

case class CreateInstanceAction(runSpecId: PathId, uuid: UUID) extends InstanceAction {
  override var affectedInstanceId: Option[Instance.Id] = None

  override def getStatusForInstanceChange(change: InstanceChange): ActionStatus.Value = {
    change.condition match {
      case Condition.Failed => ActionStatus.Failed
      case Condition.Running => ActionStatus.Succeeded
      case _ => ActionStatus.Running
    }
  }
}
