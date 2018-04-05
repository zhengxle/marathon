package mesosphere.marathon
package poc.repository

import akka.stream.scaladsl.Flow
import mesosphere.marathon.poc.repository.StateAuthority.{ Effect, MarkPersisted }

object FauxStorage {
  val fauxStorageComponent = Flow[Effect.PersistUpdates].map { e =>
    MarkPersisted(e.version)
  }
}
