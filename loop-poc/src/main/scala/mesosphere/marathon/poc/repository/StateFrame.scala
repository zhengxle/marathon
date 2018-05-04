package mesosphere.marathon
package poc.repository

case class StateFrame(
    version: Long,
    state: MarathonState)

object StateFrame {
  val empty = StateFrame(
    state = MarathonState.empty,
    version = 1)
}
