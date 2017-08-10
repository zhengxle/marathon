package mesosphere.marathon
package core.pod

import akka.{ Done, NotUsed }
import akka.stream.scaladsl.Source
import mesosphere.marathon.state.{ PathId, Timestamp }

import scala.concurrent.Future

trait PodManager {
  def ids(): Set[PathId]
  def create(p: PodDefinition, force: Boolean): Future[Done]
  def findAll(s: (PodDefinition) => Boolean): Seq[PodDefinition]
  def find(id: PathId): Option[PodDefinition]
  def update(p: PodDefinition, force: Boolean): Future[Done]
  def delete(id: PathId, force: Boolean): Future[Done]
  def versions(id: PathId): Source[Timestamp, NotUsed]
  def version(id: PathId, version: Timestamp): Future[Option[PodDefinition]]
}
