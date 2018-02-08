package mesosphere.marathon
package poc.scheduler

import akka.stream.OverflowStrategy
import akka.stream.scaladsl.{ Keep, Sink, Source }
import java.util.UUID
import mesosphere.marathon.poc.state.{ RunSpec, RunSpecRef, Instance }
import org.scalatest.Inside

class SchedulerLogicTest extends AkkaUnitTestLike with Inside {
  "signals that offers are wanted when a task is allocated" in {
    val (input, result) = Source.queue[SchedulerLogicInputEvent](16, OverflowStrategy.fail)
      .via(SchedulerLogic.eventProcesorFlow)
      .toMat(Sink.queue)(Keep.both)
      .run

    result
  }
}
