package mesosphere.marathon
package core.actions

import mesosphere.marathon.core.actions.impl.ActionManagerActor.ActionHandlerResult

import scala.concurrent.Future

trait ActionManager {
  def add(action: Action): ActionHandlerResult
  def addAsync(action: Action): Future[ActionHandlerResult]
}
