package mesosphere.marathon
package poc.repository

import akka.{ Done, NotUsed }
import akka.stream.scaladsl.Flow
import mesosphere.marathon.poc.repository.StateAuthority
import scala.concurrent.{ Future, ExecutionContext }

object FauxStorage {
  import ExecutionContext.Implicits.global
  def fakeStore(e: StateTransition): Future[Done] = {
    Future.successful(Done)
  }

  val fauxStorageComponent: Flow[StateAuthority.Effect, StateAuthority.Effect, NotUsed] = Flow[StateAuthority.Effect].
    map {
      case e: StateAuthority.Effect.CommandFailure =>
        Future.successful(e)
      case e: StateAuthority.Effect.StateUpdated =>
        Future.sequence(e.updates.map { u =>
          fakeStore(u)
        }).map { _ =>
          e
        }
    }.
    mapAsync(maxBatchesInFlight)(identity)

  val maxBatchesInFlight = 16
}
