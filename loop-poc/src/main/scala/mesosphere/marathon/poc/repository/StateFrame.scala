package mesosphere.marathon
package poc.repository
import scala.collection.immutable.Queue

case class StateFrame(
    version: Long,
    pendingUpdates: Queue[PendingUpdate],
    state: MarathonState)

object StateFrame {
  val empty = StateFrame(
    state = MarathonState.empty,
    version = 1,
    pendingUpdates = Queue.empty)
}
