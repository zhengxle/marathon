package mesosphere.marathon
package integration

import mesosphere.AkkaIntegrationTest
import mesosphere.marathon.integration.setup.EmbeddedMarathonTest
import org.scalatest.concurrent.Eventually
import play.api.libs.json.JsObject

import scala.concurrent.duration._

@IntegrationTest
class TeardownIntegrationTest extends AkkaIntegrationTest with EmbeddedMarathonTest with Eventually {

  "POST mesos/teardown should be successful" in {
    val frameworkId = marathon.info.entityJson.as[JsObject].value("frameworkId").as[String]
    Given(s"A framework with id=$frameworkId")

    And("mesos/frameworks returning this framework as active")
    val frameworks = mesos.frameworks().value
    frameworks.frameworks.exists(_.id == frameworkId) shouldBe true withClue (s"Framework with id=$frameworkId not found in active frameworks=$frameworks")

    And("mesos/teardown is called")
    val request = mesos.teardown(frameworkId)
    val status = request.status.intValue
    val entity = request.entity.toStrict(1.seconds).map(_.data.utf8String).futureValue

    Then("teardown response status should be 200")
    status shouldBe 200 withClue (s"Marathon teardown for frameworkId=$frameworkId failed with status=$status and entity=$entity")

    eventually(timeout(1.minutes), interval(2.seconds)) {
      val completed = mesos.completedFrameworkIds().value
      completed.contains(frameworkId) shouldBe true withClue (s"Havent' found framework with id=$frameworkId in the list of completed frameworks=$completed")
    }
  }
}
