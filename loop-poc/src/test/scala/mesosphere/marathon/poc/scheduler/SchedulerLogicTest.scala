package mesosphere.marathon
package poc.scheduler

import akka.stream.OverflowStrategy
import akka.stream.scaladsl.{ Keep, Sink, Source }
import java.util.UUID
import mesosphere.marathon.poc.repository.StateTransition
import mesosphere.marathon.poc.state.{ Instance, RunSpec, RunSpecRef }
import org.scalatest.Inside
import mesosphere.marathon.poc.repository.StateAuthority

class SchedulerLogicTest extends AkkaUnitTestLike with Inside {
  val sampleRunSpec = RunSpec("/lol", "blue", "2018-01-01", "sleep 100")
  val instanceId = UUID.fromString("deadbeef-c011-0123-4567-89abcdefffff")
  val sampleInstance = Instance(
    instanceId,
    sampleRunSpec.ref,
    incarnation = 1L,
    goal = Instance.Goal.Running)

  "signals that offers are wanted when a new instance is launched" in {
    val (input, result) = Source.queue[SchedulerLogicInputEvent](16, OverflowStrategy.fail)
      .via(SchedulerLogic.eventProcesorFlow())
      .toMat(Sink.queue())(Keep.both)
      .run

    input.offer(SchedulerLogicInputEvent.MarathonStateUpdate(
      Seq(
        StateTransition.RunSpecUpdated(
          sampleRunSpec.ref,
          Some(sampleRunSpec)),
        StateTransition.InstanceUpdated(
          sampleInstance.instanceId,
          Some(sampleInstance)))))

    result.pull().futureValue shouldBe SchedulerLogic.Effect.WantOffers(instanceId)
    result
  }
}
