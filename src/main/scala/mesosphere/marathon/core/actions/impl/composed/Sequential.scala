package mesosphere.marathon
package core.actions.impl.composed

import java.util.UUID

import mesosphere.marathon.core.actions.{ Action, ActionStatus, ComposedAction }

import scala.collection.mutable

case class Sequential(actions: mutable.Seq[Action], uuid: UUID) extends ComposedAction {
  override def getExecutableActions: Seq[Action] = {
    if (actions.exists(_.status == ActionStatus.Running)) {
      Seq.empty
    } else {
      actions.find(_.status == ActionStatus.Waiting).map(Seq(_)).getOrElse(Seq.empty)
    }
  }
}