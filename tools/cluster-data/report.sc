#!/usr/bin/env

import $ivy.`com.typesafe.play::play-json:2.6.7`
import $file.util
import $file.schema
import schema._
import util.Summary
import play.api.libs.json._
import java.text.DecimalFormat
import ammonite.ops._

implicit val SummaryFormat = Json.format[Summary]
implicit val HCStatFormat = Json.format[HCStat]
implicit val AgentSummaryVersionFormat = Json.format[AgentSummaryVersion]
implicit val ResourceSummaryFormat = Json.format[ResourceSummary]
implicit val MesosTaskStateFormat = Json.format[MesosTaskState]
implicit val AgentSummaryFormat = Json.format[AgentSummary]
implicit val MesosInfoFormat = Json.format[MesosInfo]
implicit val MarathonSummaryFormat = Json.format[MarathonSummary]
implicit val FrameworkSummaryFormat = Json.format[FrameworkSummary]
implicit val MesosSummaryFormat = Json.format[MesosSummary]
implicit val HealthCheckCountsFormat = Json.format[HealthCheckCounts]
implicit val HttpResponseCodeSummaryFormat = Json.format[HttpResponseCodeSummary]
implicit val LogSummaryFormat = Json.format[LogSummary]
implicit val ClusterStateFormat = Json.format[ClusterState]
val decimalFormat = new DecimalFormat

implicit class RichStringBuilder(b: StringBuilder) {
  def println(s: String): Unit = {
    b.append(s)
    b.append("\n")
  }
}

def table(headers: Seq[String], rows: Seq[Seq[String]]): String = {
  val o = new StringBuilder
  o.println("<table>")
  o.println("  <colgroup>")
  headers.foreach { _ =>
    o.println("    <col />")
  }
  o.println("  </colgroup>")
  o.println("  <tbody>")
  o.println("    <tr>")
  headers.foreach { header =>
    o.println(s"      <th>${header}</th>")
  }
  o.println("    </tr>")

  rows.foreach { values =>
    o.println("    <tr>")
    values.foreach { value =>
      o.println(s"      <td>${value}</td>")
    }
    o.println("    </tr>")
  }
  o.println("   </tbody>")
  o.println("</table>")
  o.toString
}

def miscKVTable(title: String, rows: (String, Any)*): String = {
  s"<h2>${title}</h2>\n" + table(
    Seq("Name", "Value"),
    rows.map { case (name, value) => Seq(name, value.toString) })
}

def summaryTable(title: String, rows: (String, Summary)*): String = {
  s"<h2>${title}</h2>\n" + table(
    Seq("Name", "p25", "p50", "p75", "p90", "p95", "p99", "max"),
    rows.map {
      case (name, summary) =>
        Seq(
          name,
          decimalFormat.format(summary.p25),
          decimalFormat.format(summary.p50),
          decimalFormat.format(summary.p75),
          decimalFormat.format(summary.p90),
          decimalFormat.format(summary.p95),
          decimalFormat.format(summary.p99),
          decimalFormat.format(summary.max))
    }
  )
}

@main def main(marathonLogMetricsPath: Path, clusterStatsPath: Path): Unit = {
  /*
   val marathonLogMetricsPath = pwd / "marathon-log-metrics.json"
   val clusterStatsPath = pwd / "cluster-stats.json"
   */

  val marathonLogMetrics = Json.parse(read!(marathonLogMetricsPath)).as[LogSummary]
  val clusterStats = Json.parse(read!(clusterStatsPath)).as[ClusterState]

  println(s"<div>Automated Report</div>")

  println("<h1>Marathon</h1>")

  println {
    miscKVTable("App stats",
      "Apps" -> clusterStats.rootMarathon.appCount,
      "Tasks" -> clusterStats.rootMarathon.tasksCount)
  }

  println {
    val hcs = (clusterStats.rootMarathon.healthChecks).map { hc =>
      hc.protocol -> hc.count
    }
    miscKVTable("Health Checks", hcs : _*)
  }

  println {
    summaryTable("Marathon Deployment Stats (per minute)",
      "Task Status" -> marathonLogMetrics.taskStatusCount,
      "Task Launch" -> marathonLogMetrics.taskLaunchCount,
      "Offers Processed" -> marathonLogMetrics.offersProcessedCount,
      "New Deployments" -> marathonLogMetrics.newDeploymentCount)
  }

  println {
    val rcs = marathonLogMetrics.httpResponseCodes
    summaryTable("Marathon HTTP Responses (per minute)",
      "Total bytes for all Responses" -> marathonLogMetrics.httpResponseTotalSize,
      "DCOS UI Full Group Embed Count" -> marathonLogMetrics.dcosUIFullGroupEmbedCount,
      "1xx" -> rcs.`1xx`,
      "2xx" -> rcs.`2xx`,
      "3xx" -> rcs.`3xx`,
      "4xx" -> rcs.`4xx`,
      "5xx" -> rcs.`5xx`)
  }

  println {
    summaryTable("Marathon Health Checks (the deprecated kind) (per minute)",
      "Responses" -> marathonLogMetrics.marathonHealthCounts.responses,
      "Reported Failures" -> marathonLogMetrics.marathonHealthCounts.failures)
  }

  println("<h1>Mesos</h1>")

  val mesos = clusterStats.mesos
  println {
    miscKVTable("Info",
      "Master version" -> mesos.info.version,
      "Elected time" -> java.time.Duration.ofMillis(mesos.info.electedTimeMillis.toLong)
    )
  }
  val agentSummary = mesos.agentSummary
  println {
    miscKVTable("Agents",
      "Active count" -> agentSummary.activeCount,
      "Inactive count" -> agentSummary.inactiveCount)
  }
  println {
    miscKVTable("Agent Versions",
      agentSummary.versions.map { v => v.version -> v.count } : _*)
  }

  agentSummary.resources.foreach { case (kind, resourceSummary) =>
    println {
      summaryTable(s"${kind} (per agent)",
        "Total" -> resourceSummary.total,
        "Available" -> resourceSummary.available,
        "Unreserved" -> resourceSummary.unreserved)
    }
  }
  println {
    miscKVTable("Frameworks",
      "Active" -> mesos.frameworkSummary.activeCount,
      "Inactive" -> mesos.frameworkSummary.inactiveCount)
  }
  println {
    miscKVTable("Tasks",
      mesos.taskState.map { ts => ts.state -> ts.count } : _*)
  }
  println(s"<div>Automated Report Generated on ${java.time.ZonedDateTime.now()}</div>")
}
