package mesosphere.marathon

import akka.actor._
import akka.pattern.pipe
import akka.event.{ EventStream, LoggingReceive }
import akka.stream.Materializer
import com.typesafe.scalalogging.StrictLogging
import mesosphere.marathon.core.election.{ ElectionService, LocalLeadershipEvent }
import mesosphere.marathon.core.health.HealthCheckManager
import mesosphere.marathon.core.instance.Instance
import mesosphere.marathon.core.instance.Instance.AgentInfo
import mesosphere.marathon.core.task.Task
import mesosphere.marathon.core.task.termination.{ KillReason, KillService }
import mesosphere.marathon.core.task.tracker.InstanceTracker
import mesosphere.marathon.state.{ PathId, RunSpec }
import mesosphere.marathon.storage.repository.GroupRepository
import mesosphere.marathon.stream.Implicits._
import org.apache.mesos
import org.apache.mesos.Protos.{ Status, TaskState }
import org.apache.mesos.SchedulerDriver

import scala.async.Async.{ async, await }
import scala.concurrent.{ ExecutionContext, Future }
import scala.util.control.NonFatal

class MarathonSchedulerActor private (
  groupRepository: GroupRepository,
  schedulerActions: SchedulerActions,
  historyActorProps: Props,
  healthCheckManager: HealthCheckManager,
  killService: KillService,
  marathonSchedulerDriverHolder: MarathonSchedulerDriverHolder,
  electionService: ElectionService,
  eventBus: EventStream)(implicit val mat: Materializer) extends Actor
    with StrictLogging with Stash {
  import context.dispatcher
  import mesosphere.marathon.MarathonSchedulerActor._

  /**
    * About locks:
    * - a lock is acquired if deployment is started
    * - a lock is acquired if a kill operation is executed
    * - a lock is acquired if a scale operation is executed
    *
    * This basically means:
    * - a kill/scale operation should not be performed, while a deployment is in progress
    * - a deployment should not be started, if a scale/kill operation is in progress
    * Since multiple conflicting deployment can be handled at the same time lockedRunSpecs saves
    * the lock count for each affected PathId. Lock is removed if lock count == 0.
    */
  val lockedRunSpecs = collection.mutable.Map[PathId, Int]().withDefaultValue(0)
  var historyActor: ActorRef = _
  var activeReconciliation: Option[Future[Status]] = None

  override def preStart(): Unit = {
    historyActor = context.actorOf(historyActorProps, "HistoryActor")
    electionService.subscribe(self)
  }

  override def postStop(): Unit = {
    electionService.unsubscribe(self)
  }

  def receive: Receive = suspended

  def suspended: Receive = LoggingReceive.withLabel("suspended"){
    case LocalLeadershipEvent.ElectedAsLeader =>
      logger.info("Starting scheduler actor")

    case LocalLeadershipEvent.Standby =>
    // ignored
    // FIXME: When we get this while recovering deployments, we become active anyway
    // and drop this message.

    case _ => stash()
  }

  def started: Receive = LoggingReceive.withLabel("started") {
    case LocalLeadershipEvent.Standby =>
      logger.info("Suspending scheduler actor")
      healthCheckManager.removeAll()
      lockedRunSpecs.clear()
      context.become(suspended)

    case LocalLeadershipEvent.ElectedAsLeader => // ignore

    case ReconcileTasks =>
      import akka.pattern.pipe
      import context.dispatcher
      val reconcileFuture = activeReconciliation match {
        case None =>
          logger.info("initiate task reconciliation")
          val newFuture = schedulerActions.reconcileTasks(driver)
          activeReconciliation = Some(newFuture)
          newFuture.onFailure {
            case NonFatal(e) => logger.error("error while reconciling tasks", e)
          }
          newFuture
            // the self notification MUST happen before informing the initiator
            // if we want to ensure that we trigger a new reconciliation for
            // the first call after the last ReconcileTasks.answer has been received.
            .andThen { case _ => self ! ReconcileFinished }
        case Some(active) =>
          logger.info("task reconciliation still active, reusing result")
          active
      }
      reconcileFuture.map(_ => ReconcileTasks.answer).pipeTo(sender)

    case ReconcileFinished =>
      logger.info("task reconciliation has finished")
      activeReconciliation = None

    case ReconcileHealthChecks =>
      schedulerActions.reconcileHealthChecks()

    case cmd @ KillTasks(runSpecId, tasks) =>
      @SuppressWarnings(Array("all")) /* async/await */
      def killTasks(): Future[Event] = {
        logger.debug("Received kill tasks {} of run spec {}", tasks, runSpecId)
        async {
          await(killService.killInstances(tasks, KillReason.KillingTasksViaApi))
          self ! cmd.answer
          cmd.answer
        }.recover {
          case t: Throwable =>
            CommandFailed(cmd, t)
        }
      }

      withLockFor(Set(runSpecId)) {
        killTasks().pipeTo(sender)
      } match {
        case None =>
          // KillTasks is user initiated. If we don't process it, then we should make it obvious as to why.
          logger.warn(
            s"Could not acquire lock while killing tasks ${tasks.map(_.instanceId).toList} for ${runSpecId}")
        case _ =>
      }
  }

  /**
    * Tries to acquire the lock for the given runSpecIds.
    * If it succeeds it evalutes the by name reference, returning Some(result)
    * Otherwise, returns None, which should be interpretted as lock acquisition failure
    *
    * @param runSpecIds the set of runSpecIds for which to acquire the lock
    * @param f the by-name reference that is evaluated if the lock acquisition is successful
    */
  def withLockFor[A](runSpecIds: Set[PathId])(f: => A): Option[A] = {
    // there's no need for synchronization here, because this is being
    // executed inside an actor, i.e. single threaded
    if (noConflictsWith(runSpecIds)) {
      addLocks(runSpecIds)
      Some(f)
    } else {
      None
    }
  }

  def noConflictsWith(runSpecIds: Set[PathId]): Boolean = {
    val conflicts = lockedRunSpecs.keySet intersect runSpecIds
    conflicts.isEmpty
  }

  def removeLocks(runSpecIds: Set[PathId]): Unit = runSpecIds.foreach(removeLock)
  def removeLock(runSpecId: PathId): Unit = {
    if (lockedRunSpecs.contains(runSpecId)) {
      val locks = lockedRunSpecs(runSpecId) - 1
      if (locks <= 0) lockedRunSpecs -= runSpecId else lockedRunSpecs(runSpecId) -= 1
      logger.debug(s"Removed lock for run spec: id=$runSpecId locks=$locks lockedRunSpec=$lockedRunSpecs")
    }
  }

  def addLocks(runSpecIds: Set[PathId]): Unit = runSpecIds.foreach(addLock)
  def addLock(runSpecId: PathId): Unit = {
    lockedRunSpecs(runSpecId) += 1
    logger.debug(s"Added to lock for run spec: id=$runSpecId locks=${lockedRunSpecs(runSpecId)} lockedRunSpec=$lockedRunSpecs")
  }

  // there has to be a better way...
  @SuppressWarnings(Array("OptionGet"))
  def driver: SchedulerDriver = marathonSchedulerDriverHolder.driver.get
}

object MarathonSchedulerActor {
  @SuppressWarnings(Array("MaxParameters"))
  def props(
    groupRepository: GroupRepository,
    schedulerActions: SchedulerActions,
    historyActorProps: Props,
    healthCheckManager: HealthCheckManager,
    killService: KillService,
    marathonSchedulerDriverHolder: MarathonSchedulerDriverHolder,
    electionService: ElectionService,
    eventBus: EventStream)(implicit mat: Materializer): Props = {
    Props(new MarathonSchedulerActor(
      groupRepository,
      schedulerActions,
      historyActorProps,
      healthCheckManager,
      killService,
      marathonSchedulerDriverHolder,
      electionService,
      eventBus
    ))
  }

  sealed trait Command {
    def answer: Event
  }

  case object ReconcileTasks extends Command {
    def answer: Event = TasksReconciled
  }

  private case object ReconcileFinished

  case object ReconcileHealthChecks

  case class KillTasks(runSpecId: PathId, tasks: Seq[Instance]) extends Command {
    def answer: Event = TasksKilled(runSpecId, tasks.map(_.instanceId))
  }

  sealed trait Event
  case object TasksReconciled extends Event
  case class TasksKilled(runSpecId: PathId, taskIds: Seq[Instance.Id]) extends Event
  case class CommandFailed(cmd: Command, reason: Throwable) extends Event
}

class SchedulerActions(
    groupRepository: GroupRepository,
    healthCheckManager: HealthCheckManager,
    instanceTracker: InstanceTracker,
    eventBus: EventStream,
    val killService: KillService)(implicit ec: ExecutionContext) extends StrictLogging {

  // TODO move stuff below out of the scheduler

  /**
    * Make sure all runSpecs are running the configured amount of tasks.
    *
    * Should be called some time after the framework re-registers,
    * to give Mesos enough time to deliver task updates.
    *
    * @param driver scheduler driver
    */
  @SuppressWarnings(Array("all")) // async/await
  def reconcileTasks(driver: SchedulerDriver): Future[Status] = async {
    val root = await(groupRepository.root())

    val runSpecIds = root.transitiveRunSpecsById.keySet
    val instances = await(instanceTracker.instancesBySpec())

    val knownTaskStatuses = runSpecIds.flatMap { runSpecId =>
      TaskStatusCollector.collectTaskStatusFor(instances.specInstances(runSpecId))
    }

    (instances.allSpecIdsWithInstances -- runSpecIds).foreach { unknownId =>
      logger.warn(
        s"RunSpec $unknownId exists in InstanceTracker, but not store. " +
          "The run spec was likely terminated. Will now expunge."
      )
      instances.specInstances(unknownId).foreach { orphanTask =>
        logger.info(s"Killing ${orphanTask.instanceId}")
        killService.killInstance(orphanTask, KillReason.Orphaned)
      }
    }

    logger.info("Requesting task reconciliation with the Mesos master")
    logger.debug(s"Tasks to reconcile: $knownTaskStatuses")
    if (knownTaskStatuses.nonEmpty) driver.reconcileTasks(knownTaskStatuses.asJavaCollection)

    // in addition to the known statuses send an empty list to get the unknown
    driver.reconcileTasks(java.util.Arrays.asList())
  }

  def reconcileHealthChecks(): Unit = {
    groupRepository.root().flatMap { rootGroup =>
      healthCheckManager.reconcile(rootGroup.transitiveAppsById.valuesIterator.to[Seq])
    }
  }

  def runSpecById(id: PathId): Future[Option[RunSpec]] = {
    groupRepository.root().map(_.transitiveRunSpecsById.get(id))
  }
}

/**
  * Provides means to collect Mesos TaskStatus information for reconciliation.
  */
object TaskStatusCollector {
  def collectTaskStatusFor(instances: Seq[Instance]): Seq[mesos.Protos.TaskStatus] = {
    instances.flatMap { instance =>
      instance.tasksMap.values.withFilter(task => !task.isTerminal && !task.isReserved).map { task =>
        task.status.mesosStatus.getOrElse(initialMesosStatusFor(task, instance.agentInfo))
      }
    }(collection.breakOut)
  }

  /**
    * If a task was started but Marathon never received a status update for it, it will not have a
    * Mesos TaskStatus attached. In order to reconcile the state of this task, we need to create a
    * TaskStatus and fill it with the required information.
    */
  private[this] def initialMesosStatusFor(task: Task, agentInfo: AgentInfo): mesos.Protos.TaskStatus = {
    val taskStatusBuilder = mesos.Protos.TaskStatus.newBuilder
      // in fact we haven't received a status update for these yet, we just pretend it's staging
      .setState(TaskState.TASK_STAGING)
      .setTaskId(task.taskId.mesosTaskId)

    agentInfo.agentId.foreach { agentId =>
      taskStatusBuilder.setSlaveId(mesos.Protos.SlaveID.newBuilder().setValue(agentId))
    }

    taskStatusBuilder.build()
  }

}
