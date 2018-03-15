import ammonite.ops._
import $ivy.`org.apache.spark::spark-streaming:2.2.1`
import $ivy.`org.apache.spark::spark-sql:2.2.1`
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types.{TimestampType, IntegerType}
import org.json4s.jackson.Serialization
import $file.util
import util.Summary

case class HCStat(protocol: String, count: Long)
case class MesosTaskState(state: String, count: Long)
case class AgentSummaryVersion(version: String, count: Long)
case class AgentSummary(
  activeCount: Long,
  inactiveCount: Long,
  cpus: Summary, mem: Summary, disk: Summary, gpus: Summary,
  unusedCpus: Summary, unusedMem: Summary, unusedDisk: Summary, unusedGpus: Summary,
  unreservedCpus: Summary, unreservedMem: Summary, unreservedDisk: Summary, unreservedGpus: Summary,
  versions: Seq[AgentSummaryVersion])

case class AgentInfo(
  active: Boolean,
  cpus: Double, mem: Long, disk: Long, gpus: Long,
  unusedCpus: Double, unusedMem: Long, unusedDisk: Long, unusedGpus: Long,
  unreservedCpus: Double, unreservedMem: Long, unreservedDisk: Long, unreservedGpus: Long)

case class MesosInfo(version: String, electedTime: Double)

case class MarathonSummary(appCount: Long, tasksCount: Long, healthChecks: Seq[HCStat])

case class FrameworkSummary(
  activeCount: Long,
  inactiveCount: Long)

case class MesosSummary(
  mesosInfo: MesosInfo,
  agentSummary: AgentSummary,
  frameworkSummary: FrameworkSummary)

case class ClusterState(
  rootMarathon: MarathonSummary,
  mesos: MesosSummary)

@main def main(prefix: Path): Unit = {
  val sessionFolder = s"file:${pwd}/"
  val spark = SparkSession.
    builder().
    appName("dataExtractor").
    master("local").
    config("spark.sql.warehouse.dir", sessionFolder).
    getOrCreate()

  import spark.implicits._

  val marathonApps = spark.read.json(s"${prefix}/marathon-apps.json").
    drop("env").
    cache()

  marathonApps.createOrReplaceTempView("marathon_apps")

  val marathonTasks = spark.read.json(s"${prefix}/marathon-tasks.json").
    cache()

  marathonTasks.createOrReplaceTempView("marathon_tasks")

  val mesosTasks = spark.read.json(s"${prefix}/mesos-tasks.json").
    cache()

  mesosTasks.createOrReplaceTempView("mesos_tasks")

  val mesosFrameworks = spark.read.json(s"${prefix}/mesos-frameworks.json").
    cache()

  mesosFrameworks.createOrReplaceTempView("mesos_frameworks")

  val mesosAgents = spark.read.json(s"${prefix}/mesos-agents.json").
    cache()

  mesosAgents.createOrReplaceTempView("mesos_agents")

  val mesosInfo = spark.read.json(s"${prefix}/mesos-info.json").
    cache()

  mesosInfo.createOrReplaceTempView("mesos_info")

  val marathonAppCount = spark.sql("SELECT COUNT(*) FROM marathon_apps").head.getLong(0)
  val marathonTasksCount = spark.sql("SELECT COUNT(*) FROM marathon_tasks").head.getLong(0)
  val hcStats = spark.sql(
    "SELECT hc.protocol, count(id) AS count FROM marathon_apps LATERAL VIEW explode(healthChecks) AS hc GROUP BY hc.protocol").
    as[HCStat].
    collect


  val mesosTaskStats = spark.sql("SELECT state, count(*) AS count FROM mesos_tasks GROUP BY state").
    as[MesosTaskState].
    collect


  // frameworks (inactive / active)
  val frameworkCounts = spark.sql("SELECT active, count(*) as count FROM mesos_frameworks GROUP BY active").
    as[(Boolean, Long)].
    collect.
    toMap.
    withDefault(_ => 0L)


  val agentInfos = spark.sql("""
SELECT
  active,
  resources.cpus, resources.mem, resources.disk, resources.gpus,
  resources.cpus - used_resources.cpus AS unusedCpus,
  resources.mem - used_resources.mem   AS unusedMem,
  resources.disk - used_resources.disk AS unusedDisk,
  resources.gpus - used_resources.gpus AS unusedGpus,
  unreserved_resources.cpus AS unreservedCpus,
  unreserved_resources.mem  AS unreservedMem,
  unreserved_resources.disk AS unreservedDisk,
  unreserved_resources.gpus AS unreservedGpus
FROM mesos_agents
""").
    as[AgentInfo].
    collect

  val agentVersions = spark.sql("SELECT version, count(*) AS count FROM mesos_agents GROUP BY version").
    as[AgentSummaryVersion].
    collect

  val agentSummary = AgentSummary(
    activeCount = agentInfos.count(_.active),
    inactiveCount = agentInfos.count { ai => ! ai.active },
    cpus = Summary.ofDouble(agentInfos.map(_.cpus)),
    mem = Summary.ofLong(agentInfos.map(_.mem)),
    disk = Summary.ofLong(agentInfos.map(_.disk)),
    gpus = Summary.ofLong(agentInfos.map(_.gpus)),
    unusedCpus = Summary.ofDouble(agentInfos.map(_.unusedCpus)),
    unusedMem = Summary.ofLong(agentInfos.map(_.unusedMem)),
    unusedDisk = Summary.ofLong(agentInfos.map(_.unusedDisk)),
    unusedGpus = Summary.ofLong(agentInfos.map(_.unusedGpus)),
    unreservedCpus = Summary.ofDouble(agentInfos.map(_.unreservedCpus)),
    unreservedMem = Summary.ofLong(agentInfos.map(_.unreservedMem)),
    unreservedDisk = Summary.ofLong(agentInfos.map(_.unreservedDisk)),
    unreservedGpus = Summary.ofLong(agentInfos.map(_.unreservedGpus)),
    versions = agentVersions)

  // Mesos master version
  val mesosInfoR = spark.sql("select version, elected_time AS electedTime FROM mesos_info").as[MesosInfo].take(1).head


  val cs = ClusterState(
    rootMarathon = MarathonSummary(
      appCount = marathonAppCount,
      tasksCount = marathonTasksCount,
      healthChecks = hcStats),
    mesos = MesosSummary(
      mesosInfo = mesosInfoR,
      agentSummary = agentSummary,
      frameworkSummary = FrameworkSummary(
        inactiveCount = frameworkCounts(false),
        activeCount = frameworkCounts(true))))

  implicit val formats = org.json4s.DefaultFormats
  println(Serialization.write(cs))
}
