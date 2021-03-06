package mesosphere.marathon
package integration

import mesosphere.marathon.core.instance.Instance
import mesosphere.marathon.core.task.Task
import mesosphere.{AkkaIntegrationTest, WhenEnvSet}
import mesosphere.marathon.integration.facades.ITEnrichedTask
import mesosphere.marathon.integration.setup._
import mesosphere.marathon.raml.App
import mesosphere.marathon.state.PathId._
import mesosphere.marathon.state.UnreachableDisabled
import org.scalatest.Inside

import scala.concurrent.duration._

class TaskUnreachableIntegrationTest extends AkkaIntegrationTest with EmbeddedMarathonTest with Inside {

  override lazy val mesosNumMasters = 1
  override lazy val mesosNumSlaves = 2

  override val marathonArgs: Map[String, String] = Map(
    "reconciliation_initial_delay" -> "5000",
    "reconciliation_interval" -> "5000",
    "scale_apps_initial_delay" -> "5000",
    "scale_apps_interval" -> "5000",
    "min_revive_offers_interval" -> "100",
    "task_lost_expunge_initial_delay" -> "1000",
    "task_lost_expunge_interval" -> "1000"
  )

  // TODO unreachable tests for pods

  before {
    // Every test below expects 1 running and 1 stopped agent
    mesosCluster.agents.head.start()
    mesosCluster.agents(1).stop()
    mesosCluster.waitForLeader().futureValue
    cleanUp()
  }

  override def afterAll(): Unit = {
    // We need to start all the agents for the teardown to be able to kill all the (UNREACHABLE) executors/tasks
    mesosCluster.agents.foreach(_.start())
    eventually { mesosCluster.state.value.agents.size shouldBe mesosCluster.agents.size }
    super.afterAll()
  }

  "TaskUnreachable" should {
    "A task unreachable update will trigger a replacement task" in {
      Given("a new app with proper timeouts")
      val strategy = raml.UnreachableEnabled(inactiveAfterSeconds = 10, expungeAfterSeconds = 5 * 60)
      val app = appProxy(testBasePath / "unreachable-with-eventual-replacement", "v1", instances = 1, healthCheck = None).copy(
        unreachableStrategy = Option(strategy)
      )
      waitForDeployment(marathon.createAppV2(app))
      val task = waitForTasks(app.id.toPath, 1).head

      When("the slave is partitioned")
      mesosCluster.agents(0).stop()

      Then("the task is declared unreachable")
      waitForEventMatching("Task is declared unreachable") {
        matchEvent("TASK_UNREACHABLE", task)
      }

      And("the task is declared unreachable inactive")
      waitForEventWith("instance_changed_event", _.info("condition") == "UnreachableInactive", s"event instance_changed_event (UnreachableInactive) to arrive")

      And("a replacement task is started on a different slave")
      mesosCluster.agents(1).start() // Start an alternative slave
      waitForStatusUpdates("TASK_RUNNING")
      val tasks = marathon.tasks(app.id.toPath).value
      tasks should have size 2
      tasks.groupBy(_.state).keySet should be(Set("TASK_RUNNING", "TASK_UNREACHABLE"))
      val replacement = tasks.find(_.state == "TASK_RUNNING").get

      When("the first slaves comes back")
      mesosCluster.agents(0).start()

      Then("the task reappears as running")
      waitForEventMatching("Task is declared running") {
        matchEvent("TASK_RUNNING", task)
      }

      And("the replacement task is killed")
      waitForEventMatching("Replacement task is killed") {
        matchEvent("TASK_KILLED", replacement)
      }

      And("there is only one running task left")
      marathon.tasks(app.id.toPath).value should have size 1
      marathon.tasks(app.id.toPath).value.head.state should be("TASK_RUNNING")
    }

    "A task unreachable update with inactiveAfterSeconds 0 will trigger a replacement task instantly" in {
      Given("a new app with proper timeouts")
      val strategy = raml.UnreachableEnabled(inactiveAfterSeconds = 0, expungeAfterSeconds = 60)
      val app = appProxy(testBasePath / "unreachable-with-instant-replacement", "v1", instances = 1, healthCheck = None).copy(
        unreachableStrategy = Option(strategy)
      )
      waitForDeployment(marathon.createAppV2(app))
      val task = waitForTasks(app.id.toPath, 1).head

      When("the slave is partitioned")
      mesosCluster.agents(0).stop()
      mesosCluster.agents(1).start() // Start an alternative agent

      Then("the task is declared unreachable")
      waitForEventMatching("Task is declared unreachable") {
        matchEvent("TASK_UNREACHABLE", task)
      }

      Then("the replacement task is running")
      // wait not longer than 1 second, because it should be replaced even faster
      waitForEventMatching("Replacement task is declared running", 6.seconds) {
        matchEvent("TASK_RUNNING", app)
      }

      // immediate replacement should be started
      val tasks = marathon.tasks(app.id.toPath).value
      tasks should have size 2
      tasks.groupBy(_.state).keySet should be(Set("TASK_RUNNING", "TASK_UNREACHABLE"))
    }

    // regression test for https://github.com/mesosphere/marathon/issues/4059
    "Scaling down an app with constraints and unreachable task will succeed" in {
      Given("an app that is constrained to a unique hostname")
      val constraint = Set(Seq("node", "MAX_PER", "1"))

      // start both slaves
      mesosCluster.agents.foreach(_.start())

      val strategy = raml.UnreachableEnabled(inactiveAfterSeconds = 3 * 60, expungeAfterSeconds = 4 * 60)
      val app = appProxy(testBasePath / "unreachable-with-constraints-scaling", "v1", instances = 2, healthCheck = None)
        .copy(constraints = constraint, unreachableStrategy = Option(strategy))

      waitForDeployment(marathon.createAppV2(app))
      val enrichedTasks = waitForTasks(app.id.toPath, num = 2)
      val clusterState = mesosCluster.state.value
      val slaveId = clusterState.agents.find(_.attributes.attributes("node").toString.toDouble.toInt == 0).getOrElse(
        throw new RuntimeException(s"failed to find agent1: attributes by agent=${clusterState.agents.map(_.attributes.attributes)}")
      )
      val task = enrichedTasks.find(t => t.slaveId.contains(slaveId.id)).getOrElse(
        throw new RuntimeException("No matching task found on slave1")
      )

      When("agent1 is stopped")
      mesosCluster.agents.head.stop()
      Then("one task is declared unreachable")
      waitForEventMatching("Task is declared lost") {
        matchEvent("TASK_UNREACHABLE", task)
      }

      And("the task is not removed from the task list")
      inside(waitForTasks(app.id.toPath, num = 2)) {
        case tasks =>
          tasks should have size 2
          tasks.exists(_.state == "TASK_UNREACHABLE") shouldBe true
      }

      When("we try to scale down to one instance")
      val update = marathon.updateApp(app.id.toPath, raml.AppUpdate(instances = Some(1)))
      waitForEventMatching("deployment to scale down should be triggered") {
        matchDeploymentStart(app.id)
      }

      Then("the update deployment will eventually finish")
      waitForDeployment(update)

      And("The unreachable task is expunged")
      eventually(inside(marathon.tasks(app.id.toPath).value) {
        case t :: Nil =>
          t.state shouldBe "TASK_RUNNING"
      })

      marathon.listDeploymentsForBaseGroup().value should have size 0
    }

    "wipe pod instances with persistent volumes" in {

      Given("a pod with persistent volumes")
      val pod = residentPod("resident-pod-with-one-instance-wipe").copy(
        instances = 1
      )

      When("The pod is created")
      val createResult = marathon.createPodV2(pod)
      createResult should be(Created)
      waitForDeployment(createResult)
      val taskId = marathon.podTasksIds(pod.id).head
      eventually { marathon.status(pod.id) should be(Stable) }

      Then("1 instance should be running")
      val status = marathon.status(pod.id)
      status should be(OK)
      status.value.instances should have size 1
      mesosCluster.agents(1).start()

      When("An instance is unreachable")
      mesosCluster.agents(0).stop()
      waitForEventMatching("Task is declared unreachable") {
        matchEvent("TASK_UNREACHABLE", taskId)
      }

      And("Pods instance is deleted")
      val instanceId = status.value.instances.head.id
      val deleteResult = marathon.deleteInstance(pod.id, instanceId, wipe = true)
      deleteResult should be(OK)

      Then("pod instance is erased from marathon's knowledge ")
      val knownInstanceIds = marathon.status(pod.id).value.instances.map(_.id)
      eventually {
        knownInstanceIds should not contain instanceId
      }

      And("a new pod with a new persistent volume is scheduled")
      waitForStatusUpdates("TASK_RUNNING")
      marathon.status(pod.id).value.instances should have size 1

      When("the task associated with pod becomes reachable again")
      mesosCluster.agents(0).start()

      Then("Marathon kills the task and removes the associated reservation and volume")
      waitForEventMatching("Task is declared killed") {
        matchUnknownTerminatedEvent(Task.Id(taskId).instanceId)
      }
    }

    "wipe pod instances without persistent volumes" in {
      Given("a pod with persistent volumes")
      val pod = simplePod("simple-pod-with-one-instance-wipe").copy(
        instances = 1,
        unreachableStrategy = UnreachableDisabled // this test is flaky without this but it's not test's fault
      )

      When("The pod is created")
      val createResult = marathon.createPodV2(pod)
      createResult should be(Created)
      waitForDeployment(createResult)
      val taskId = marathon.podTasksIds(pod.id).head
      eventually { marathon.status(pod.id) should be(Stable) }

      Then("1 instance should be running")
      val status = marathon.status(pod.id)
      status should be(OK)
      status.value.instances should have size 1
      mesosCluster.agents(1).start()
      eventually {
        mesos.state.value.agents.size shouldEqual 2
      }

      When("An instance is unreachable")
      mesosCluster.agents(0).stop()
      waitForEventMatching("Task is declared unreachable") {
        matchEvent("TASK_UNREACHABLE", taskId)
      }

      And("Pods instance is deleted")
      val instanceId = status.value.instances.head.id
      val deleteResult = marathon.deleteInstance(pod.id, instanceId, wipe = true)
      deleteResult should be(OK)

      Then("pod instance is erased from marathon's knowledge ")
      val knownInstanceIds = marathon.status(pod.id).value.instances.map(_.id)
      eventually {
        knownInstanceIds should not contain instanceId
      }

      And("a new pod with is scheduled")
      waitForStatusUpdates("TASK_RUNNING")
      marathon.status(pod.id).value.instances should have size 1

      When("the task associated with pod becomes reachable again")
      mesosCluster.agents(0).start()

      Then("Marathon kills the task")
      waitForEventMatching("Task is declared killed") {
        matchUnknownTerminatedEvent(Task.Id(taskId).instanceId)
      }
    }
  }

  def matchEvent(status: String, app: App): CallbackEvent => Boolean = { event =>
    event.info.get("taskStatus").contains(status) &&
      event.info.get("appId").contains(app.id)
  }

  def matchUnknownTerminatedEvent(instanceId: Instance.Id): CallbackEvent => Boolean = { event =>
    event.eventType == "unknown_instance_terminated_event" && event.info.get("instanceId").contains(instanceId.idString)
  }
  def matchEvent(status: String, task: ITEnrichedTask): CallbackEvent => Boolean =
    matchEvent(status, task.id)

  def matchEvent(status: String, taskId: String): CallbackEvent => Boolean = { event =>
    event.info.get("taskStatus").contains(status) &&
      event.info.get("taskId").contains(taskId)
  }

  private def matchDeploymentStart(appId: String): CallbackEvent => Boolean = { event =>
    val infoString = event.info.toString()
    event.eventType == "deployment_info" && matchScaleApplication(infoString, appId)
  }

  private def matchScaleApplication(infoString: String, appId: String): Boolean = {
    infoString.contains(s"List(Map(actions -> List(Map(action -> ScaleApplication, app -> $appId))))")
  }
}
