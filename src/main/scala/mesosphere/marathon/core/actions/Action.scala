package mesosphere.marathon
package core.actions

import java.util.UUID

trait Action {
  val uuid: UUID
  var status: ActionStatus.Value = ActionStatus.Waiting
  def setStatus(newStatus: ActionStatus.Value): Unit = status = newStatus
}