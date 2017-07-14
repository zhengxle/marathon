package mesosphere.marathon
package core.actions

trait ComposedAction extends Action {
  def getExecutableActions: Seq[Action]
}
