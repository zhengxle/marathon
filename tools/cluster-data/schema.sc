import $file.util
import util.Summary

case class MesosTaskState(state: String, count: Long)
case class HCStat(protocol: String, count: Long)
case class AgentSummaryVersion(version: String, count: Long)
case class ResourceSummary(
  total: Summary,
  available: Summary,
  unreserved: Summary)

case class AgentSummary(
  activeCount: Long,
  inactiveCount: Long,
  resources: Map[String, ResourceSummary],
  versions: Seq[AgentSummaryVersion])

case class MesosInfo(version: String, electedTimeMillis: Double)

case class MarathonSummary(appCount: Long, tasksCount: Long, healthChecks: Seq[HCStat])

case class FrameworkSummary(
  activeCount: Long,
  inactiveCount: Long)

case class MesosSummary(
  info: MesosInfo,
  agentSummary: AgentSummary,
  frameworkSummary: FrameworkSummary,
  taskState: Seq[MesosTaskState]
)


case class HealthCheckCounts(
  responses: Summary,
  failures: Summary)

case class HttpResponseCodeSummary(
  `1xx`: Summary,
  `2xx`: Summary,
  `3xx`: Summary,
  `4xx`: Summary,
  `5xx`: Summary)

case class LogSummary(
  frameSizeMs: Int,
  frames: Int,
  detectedFailovers: Int,
  newDeploymentCount: Summary,
  httpResponseTotalSize: Summary,
  taskStatusCount: Summary,
  taskLaunchCount: Summary,
  offersProcessedCount: Summary,
  marathonHealthCounts: HealthCheckCounts,
  dcosUIFullGroupEmbedCount: Summary,
  httpResponseCodes: HttpResponseCodeSummary)

case class ClusterState(
  rootMarathon: MarathonSummary,
  mesos: MesosSummary)
