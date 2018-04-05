package mesosphere.marathon
package poc.repository

case class PendingUpdate(version: Long, requestId: Long, updates: Seq[StateTransition])
