package mesosphere.marathon
package poc

import akka.stream.FlowShape
import akka.{ Done, NotUsed }
import akka.stream.scaladsl.{ Broadcast, Flow, MergePreferred }
import akka.stream.{ ClosedShape, OverflowStrategy }
import akka.stream.scaladsl.{ Flow, GraphDSL }
import java.util.UUID

import akka.actor.ActorSystem
import akka.stream.{ ActorMaterializer }
import akka.stream.scaladsl.{ Sink, Source }
import com.typesafe.scalalogging.StrictLogging
import mesosphere.AkkaUnitTest
import mesosphere.marathon.IntegrationTest
import mesosphere.marathon.integration.setup.MesosClusterTest
import mesosphere.marathon.poc.repository.StateAuthority
import mesosphere.marathon.poc.repository.StateAuthority.{ Effect, MarkPersisted, StateAuthorityInputEvent }
import mesosphere.marathon.poc.scheduler.{ SchedulerLogic, SchedulerLogicInputEvent }
import mesosphere.mesos.conf.MesosClientConf
import org.apache.mesos.v1.mesos.{ Filters, FrameworkID, FrameworkInfo }
import org.apache.mesos.v1.scheduler.scheduler.Event
import org.scalatest.Inside
import org.scalatest.concurrent.Eventually
import scala.annotation.tailrec
import scala.concurrent.Future
import mesosphere.mesos.client.MesosClient
import mesosphere.marathon.poc.repository.FauxStorage

@IntegrationTest
class SchedulerIntegrationTest extends AkkaUnitTest
  with MesosClusterTest
  with Eventually
  with Inside
  with StrictLogging {

  def commandProcessorFlowWithStorage(storage: Flow[Effect.PersistUpdates, MarkPersisted, NotUsed]): Flow[StateAuthorityInputEvent, Effect, NotUsed] = {
    Flow.fromGraph {
      GraphDSL.create(storage) { implicit b =>
        { storage =>
          import GraphDSL.Implicits._
          val inputEvents = b.add(MergePreferred[StateAuthorityInputEvent](1, true))
          val effectBroadcast = b.add(Broadcast[Effect](2))
          val stateProcessor = b.add(StateAuthority.commandProcessorFlow)

          inputEvents.out ~> stateProcessor

          stateProcessor ~> effectBroadcast

          effectBroadcast.out(0).collect { case e: Effect.PersistUpdates => e } ~> storage ~>
            StateAuthority.markPersistedConflator ~> inputEvents.in(0)

          FlowShape(inputEvents.in(1), effectBroadcast.out(1))
        }
      }
    }
  }

  "it launches a task" in {
    val inputSource = Source.queue[StateAuthorityInputEvent](16, OverflowStrategy.fail)
    val getUpdates = Flow[Effect].collect {
      case e: Effect.PublishUpdates =>
        SchedulerLogicInputEvent.MarathonStateUpdate(e.updates)
    }

    val router = GraphDSL.create(inputSource) { implicit b =>
      { inputShape =>
        import GraphDSL.Implicits._

        val stateProcessorWithStorage = b.add(commandProcessorFlowWithStorage(FauxStorage.fauxStorageComponent))
        val scheduler = b.add(SchedulerLogic.eventProcesorFlow())

        inputShape ~> stateProcessorWithStorage ~> getUpdates ~> scheduler

        ClosedShape
      }
    }
  }

  class Fixture(existingFrameworkId: Option[FrameworkID] = None) {
    implicit val system: ActorSystem = ActorSystem()
    implicit val materializer: ActorMaterializer = ActorMaterializer()

    val frameworkInfo = FrameworkInfo(
      user = "foo",
      name = "Example FOO Framework",
      id = existingFrameworkId,
      roles = Seq("foo"),
      failoverTimeout = Some(0.0f),
      capabilities = Seq(FrameworkInfo.Capability(`type` = Some(FrameworkInfo.Capability.Type.MULTI_ROLE))))

    val mesosUrl = new java.net.URI(mesos.url)
    val mesosHost = mesosUrl.getHost
    val mesosPort = mesosUrl.getPort

    val conf = new MesosClientConf(master = s"${mesosUrl.getHost}:${mesosUrl.getPort}")
    val client = MesosClient(conf, frameworkInfo).runWith(Sink.head).futureValue

    val queue = client.mesosSource.
      runWith(Sink.queue())

    /**
      * Pull (and drop) elements from the queue until the predicate returns true. Does not cancel the upstream.
      *
      * Returns Some(element) when queue emits an event which matches the predicate
      * Returns None if queue ends (client closes) before the predicate matches
      * TimeoutException is thrown if no event is available within the `patienceConfig.timeout` duration.
      *
      * @param predicate Function to evaluate to see if event matches
      * @return matching event, if any
      */
    @tailrec final def pullUntil(predicate: Event => Boolean): Option[Event] =
      queue.pull().futureValue match {
        case e @ Some(event) if (predicate(event)) =>
          e
        case None =>
          None
        case _ =>
          pullUntil(predicate)
      }
  }

}
