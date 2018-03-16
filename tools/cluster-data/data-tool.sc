import ammonite.ops._
import $ivy.`org.apache.spark::spark-streaming:2.2.1`
import $ivy.`org.apache.spark::spark-sql:2.2.1`
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types.{TimestampType, IntegerType}
import org.json4s.jackson.Serialization
import $file.util
import $file.schema
import util.Summary
import schema.{LogSummary, HttpResponseCodeSummary, HealthCheckCounts}

val FrameDurationMS = (60 * 1000)

case class LogRow(
  timestamp: java.sql.Timestamp,
  _class: Option[String],
  class2: Option[String],
  host: String,
  size: Option[Long], // httpSize
  status: Option[String], // health Check healthy
  httpStatus: Option[Long], // http response code
  uri: Option[String],
  params: Option[String]
)

case class FrameSummary(
  windowStart: Long, // inclusive
  host: String,
  newDeploymentCount: Int,
  httpResponseTotalSize: Long,
  taskStatusCount: Int,
  taskLaunchCount: Int,
  offersProcessedCount: Int,
  marathonHealthCheckResponseCount: Int,
  marathonHealthCheckFailCount: Int,
  dcosUIFullGroupEmbedCount: Int,
  httpStatus1xx: Int,
  httpStatus2xx: Int,
  httpStatus3xx: Int,
  httpStatus4xx: Int,
  httpStatus5xx: Int
)

def matchesClass(class1: String): LogRow => Boolean = { message =>
  message._class.contains(class1)
}

def matchesClass(class1: String, class2: String): LogRow => Boolean = { message =>
  message._class.contains(class1) && message.class2.contains(class2)
}
/**
  * Given a sorted list of leader candidates for a series of frames, fill in any consecutive gaps where no leader is
  * known, but the previous leader is still reporting.
  *
  * If a frame contains a leader transition such two hosts log leader-like messages, then we assume neither are the
  * leader for the frame.
  */
def deduceLeader(sortedLeaderCandidates: Seq[(Long, Seq[(String, Boolean)])]): (Map[Long, String], Int) = {
  val leadersBuilder = Map.newBuilder[Long, String]
  var currentLeader: Option[String] = None
  var totalLeaders = 0
  sortedLeaderCandidates.foreach { case (window, candidates) =>
    val windowLeaders = candidates.collect { case (candidate, true) => candidate }
    windowLeaders match {
      case candidate :: Nil =>
        // indisupted leader
        if (!currentLeader.contains(candidate)) {
          currentLeader = Some(candidate)
          totalLeaders += 1
        }
        leadersBuilder += window -> candidate
      case l if l.length > 1 => // multiple candidates
                                // transitioning
        currentLeader = None
      case Nil =>
        // unknown
        // only assume leader if still reporting for this window
        currentLeader match {
          case Some(l) if candidates.exists { case (host, knownToBeLeader) => l == host } =>
            leadersBuilder += window -> l
          case _ =>
            // previous leader did not report any log messages this frame. Do not mark any node a leader for this frame.
        }
    }
  }
  (leadersBuilder.result, totalLeaders)
}

@main def main(inputJsonFile: Path): Unit = {
  // val inputJsonFile = pwd / "marathon-logs.json.ld"
  val sessionFolder = s"file:${inputJsonFile/up}/"
  System.err.println(s"sessionFolder = ${sessionFolder}")
  val spark = SparkSession.
    builder().
    appName("dataExtractor").
    master("local").
    config("spark.sql.warehouse.dir", sessionFolder).
    getOrCreate()

  import spark.implicits._

  System.err.println(s"json file = ${inputJsonFile.last}")
  val odf = spark.read.json(inputJsonFile.toString)

  val df = odf.
    withColumnRenamed("class", "_class").
    drop("message").
    withColumn("@timestamp", odf.col("@timestamp").cast(TimestampType)).
    withColumnRenamed("@timestamp", "timestamp").
    as[LogRow]


  val grouped = df.groupByKey { r =>
    ((r.timestamp.getTime / (FrameDurationMS)), r.host)
  }.mapGroups { case (k, v) => (k, v.filter(_._class.nonEmpty).toList) }.cache()

  // These message classes are only emitted by the Marathon leader
  val leaderLogClasses: Set[String] = Set(
    "deploy", "gc", "health", "instance", "offer", "reconcile", "scale-check", "tasks")

  def hasLeaderLogMessage(messages: Iterable[LogRow]) =
    messages.exists { m =>
      m._class.forall(leaderLogClasses contains _)
    }

  val leaderCandidates = grouped.map { case ((window, host), messages) => (window, host, hasLeaderLogMessage(messages)) }.
    groupByKey { g: (Long, String, Boolean) => g._1 }.
    mapGroups { case (window, windowHosts: Iterator[(Long, String, Boolean)]) =>
      window -> windowHosts.map { case (_, host, isLeader) => host -> isLeader }.toList
    }.
    collect.
    sortBy(_._1)


  val (leaders, totalLeaders) = deduceLeader(leaderCandidates)

  val leaderFrames = grouped.
    filter { _ match {
      case ((frame, host), messages) => leaders.get(frame).contains(host)
    } }.
    map { case ((frame, host), messages) =>
      val httpStatuses = messages.iterator.filter(matchesClass("http", "response")).flatMap(_.httpStatus).toSeq

      FrameSummary(
        windowStart = frame * FrameDurationMS,
        host = host,
        newDeploymentCount = messages.iterator.filter(matchesClass("deploy", "new")).size,
        httpResponseTotalSize = messages.iterator.filter(matchesClass("http", "response")).foldLeft(0L) { (r, logRow) =>
          r + logRow.size.getOrElse(0L)
        },
        taskStatusCount = messages.iterator.filter(matchesClass("tasks", "status")).size,
        taskLaunchCount = messages.iterator.filter(matchesClass("tasks", "launch")).size,
        offersProcessedCount = messages.iterator.filter(matchesClass("offer", "processed")).size,
        marathonHealthCheckResponseCount = messages.iterator.filter(matchesClass("health", "result")).size,
        marathonHealthCheckFailCount = messages.iterator.filter(matchesClass("health", "result")).filter(_.status != Some("Healthy")).size,
        dcosUIFullGroupEmbedCount = messages.iterator.filter(matchesClass("http", "response")).filter { r =>
          r.uri == Some("/v2/groups") && r.params.exists { p => p.contains("_timestamp") && p.contains("embed=group.apps.deployments") }
        }.size,
        httpStatus1xx = httpStatuses.count { s => s >= 100 && s < 200 },
        httpStatus2xx = httpStatuses.count { s => s >= 200 && s < 300 },
        httpStatus3xx = httpStatuses.count { s => s >= 300 && s < 400 },
        httpStatus4xx = httpStatuses.count { s => s >= 400 && s < 500 },
        httpStatus5xx = httpStatuses.count { s => s >= 500 && s < 600 })
    }.collect

  val taskStatusSummary = Summary.ofInt(leaderFrames.map(_.taskStatusCount))
  val httpResponseTotalSizeSummary = Summary.ofLong(leaderFrames.map(_.httpResponseTotalSize))

  // val httpSizeSlopes = (sizes.iterator).sliding(2).map { case Seq(a, b) => (a, (b-a)/(a+1)) }
  // .foldLeft((0L,0L)) { case (r@(rThreshold, rDeltaMult), t@(threshold, deltaMult)) => if (deltaMult > rDeltaMult) t else r }
  // val nonEmptyLeaderFrames = leaderFrames.filter(_.taskStatusCount > (taskStatusSummary.p50 / 2)) // sometimes we get idle frames in Marathon. Looking at task status count medians helps to filter these out

  val logSummary = LogSummary(
    frameSizeMs = FrameDurationMS,
    frames = leaderFrames.length,
    detectedFailovers = totalLeaders - 1,
    newDeploymentCount = Summary.ofInt(leaderFrames.map(_.newDeploymentCount)),
    httpResponseTotalSize = httpResponseTotalSizeSummary,
    taskStatusCount = taskStatusSummary,
    taskLaunchCount = Summary.ofInt(leaderFrames.map(_.taskLaunchCount)),
    offersProcessedCount = Summary.ofInt(leaderFrames.map(_.offersProcessedCount)),
    marathonHealthCounts = HealthCheckCounts(
      responses = Summary.ofInt(leaderFrames.map(_.marathonHealthCheckResponseCount)),
      failures = Summary.ofInt(leaderFrames.map(_.marathonHealthCheckFailCount))),
    dcosUIFullGroupEmbedCount = Summary.ofInt(leaderFrames.map(_.dcosUIFullGroupEmbedCount)),
    httpResponseCodes = HttpResponseCodeSummary(
      `1xx` = Summary.ofInt(leaderFrames.map(_.httpStatus1xx)),
      `2xx` = Summary.ofInt(leaderFrames.map(_.httpStatus2xx)),
      `3xx` = Summary.ofInt(leaderFrames.map(_.httpStatus3xx)),
      `4xx` = Summary.ofInt(leaderFrames.map(_.httpStatus4xx)),
      `5xx` = Summary.ofInt(leaderFrames.map(_.httpStatus5xx)))
  )

  implicit val formats = org.json4s.DefaultFormats
  println(Serialization.write(logSummary))
}
