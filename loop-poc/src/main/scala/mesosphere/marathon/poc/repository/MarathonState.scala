package mesosphere.marathon
package poc.repository

import mesosphere.marathon.poc.state.{ RootGroup, InstanceSet }

case class MarathonState(
    rootGroup: RootGroup,
    instances: InstanceSet)

object MarathonState {
  def empty = MarathonState(rootGroup = RootGroup.empty, instances = InstanceSet.empty)
}
