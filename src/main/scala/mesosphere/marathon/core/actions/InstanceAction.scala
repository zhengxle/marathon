package mesosphere.marathon
package core.actions

import mesosphere.marathon.core.instance.Instance
import mesosphere.marathon.core.instance.update.InstanceChange

trait InstanceAction extends Action {
  var affectedInstanceId: Option[Instance.Id]

  def getStatusForInstanceChange(change: InstanceChange): ActionStatus.Value

  def updateStatusForInstanceChange(instanceChange: InstanceChange): Unit =
    this.setStatus(this.getStatusForInstanceChange(instanceChange))
}