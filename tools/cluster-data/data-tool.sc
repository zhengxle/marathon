import ammonite.ops._
import $ivy.`org.apache.spark::spark-streaming:2.2.1`
import $ivy.`org.apache.spark::spark-sql:2.2.1`
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types.{TimestampType, IntegerType}
import org.json4s.jackson.Serialization

val FrameDurationMS = (60 * 1000)

case class LogRow(
  timestamp: java.sql.Timestamp,
  _class: Option[String],
  class2: Option[String],
  host: String,
  size: Option[Long], // httpSize
  status: Option[String] // health Check healthy
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
  marathonHealthCheckFailCount: Int
)

case class Summary(
  p25: BigDecimal,
  p50: BigDecimal,
  p75: BigDecimal,
  p90: BigDecimal,
  p95: BigDecimal,
  p99: BigDecimal,
  max: BigDecimal)

object Summary {
  private val percentiles: Seq[Double] = Seq(0.25, 0.5, 0.75, 0.90, 0.95, 0.99, 1.0)
  def ofInt(data: Seq[Int]): Summary = {
    val result = getPercentilesInt(data, percentiles)
    Summary(result(0), result(1), result(2), result(3), result(4), result(5), result(6))
  }
  def ofLong(data: Seq[Long]): Summary = {
    val result = getPercentilesLong(data, percentiles)
    Summary(result(0), result(1), result(2), result(3), result(4), result(5), result(6))
  }

  private def getPercentilesInt(data: Seq[Int], percentiles: Seq[Double]): Seq[Int] = {
    val sorted = data.sorted
    val lastElement = data.sorted.length - 1
    percentiles.map { percentile =>
      sorted((percentile * lastElement).toInt)
    }
  }

  private def getPercentilesLong(data: Seq[Long], percentiles: Seq[Double]): Seq[Long] = {
    val sorted = data.sorted
    val lastElement = data.sorted.length - 1
    percentiles.map { percentile =>
      sorted((percentile * lastElement).toInt)
    }
  }
}

def matchesClass(class1: String): LogRow => Boolean = { message =>
  message._class.contains(class1)
}

def matchesClass(class1: String, class2: String): LogRow => Boolean = { message =>
  message._class.contains(class1) && message.class2.contains(class2)
}


case class LogSummary(
  frameSizeMs: Int,
  frames: Int,
  detectedFailovers: Int,
  newDeploymentCount: Summary,
  httpResponseTotalSize: Summary,
  taskStatusCount: Summary,
  taskLaunchCount: Summary,
  offersProcessedCount: Summary,
  marathonHealthCheckResponseCount: Summary,
  marathonHealthCheckFailCount: Summary
)

@main def main(inputJsonFile: Path): Unit = {
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

  val (leaders, totalLeaders) = deduceLeader(leaderCandidates)

  leaders.filter { case (_, hosts) => hosts.length == 1 }

  val leaderFrames = grouped.
    filter { _ match {
      case ((frame, host), messages) => leaders.get(frame).contains(host)
    } }.
    map { case ((frame, host), messages) =>
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
        marathonHealthCheckFailCount = messages.iterator.filter(matchesClass("health", "result")).filter(_.status != Some("Healthy")).size
      )
    }.collect

  val logSummary = LogSummary(
    frameSizeMs = FrameDurationMS,
    frames = leaderFrames.length,
    detectedFailovers = totalLeaders - 1,
    newDeploymentCount = Summary.ofInt(leaderFrames.map(_.newDeploymentCount)),
    httpResponseTotalSize = Summary.ofLong(leaderFrames.map(_.httpResponseTotalSize)),
    taskStatusCount = Summary.ofInt(leaderFrames.map(_.taskStatusCount)),
    taskLaunchCount = Summary.ofInt(leaderFrames.map(_.taskLaunchCount)),
    offersProcessedCount = Summary.ofInt(leaderFrames.map(_.offersProcessedCount)),
    marathonHealthCheckResponseCount = Summary.ofInt(leaderFrames.map(_.marathonHealthCheckResponseCount)),
    marathonHealthCheckFailCount = Summary.ofInt(leaderFrames.map(_.marathonHealthCheckFailCount)))

  implicit val formats = org.json4s.DefaultFormats
  println(Serialization.write(logSummary))
}
