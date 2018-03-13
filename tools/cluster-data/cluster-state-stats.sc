import ammonite.ops._
import $ivy.`org.apache.spark::spark-streaming:2.2.1`
import $ivy.`org.apache.spark::spark-sql:2.2.1`
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types.{TimestampType, IntegerType}
import org.json4s.jackson.Serialization

val sessionFolder = s"file:${pwd}/"
val spark = SparkSession.
  builder().
  appName("dataExtractor").
  master("local").
  config("spark.sql.warehouse.dir", sessionFolder).
  getOrCreate()

import spark.implicits._

val marathonApps = spark.read.json("target/cerner-2018-02-23/scratch/marathon-apps.json").
  drop("env").
  cache()

marathonApps.createOrReplaceTempView("marathon_apps")

val marathonTasks = spark.read.json("target/cerner-2018-02-23/scratch/marathon-tasks.json").
  cache()

marathonTasks.createOrReplaceTempView("marathon_tasks")

val mesosTasks = spark.read.json("target/cerner-2018-02-23/scratch/mesos-tasks.json").
  cache()

mesosTasks.createOrReplaceTempView("mesos_tasks")

val mesosAgents = spark.read.json("target/cerner-2018-02-23/scratch/mesos-agents.json").
  cache()

mesosAgents.createOrReplaceTempView("mesos_agents")

val marathonAppCount = spark.sql("SELECT COUNT(*) FROM marathon_apps").head.getLong(0)
val marathonTasksCount = spark.sql("SELECT COUNT(*) FROM marathon_tasks").head.getLong(0)
case class HCStat(protocol: String, count: Long)
val hcStats = spark.sql(
  "SELECT hc.protocol, count(id) AS count FROM marathon_apps LATERAL VIEW explode(healthChecks) AS hc GROUP BY hc.protocol").
  as[HCStat].
  collect


case class MesosTaskState(state: String, count: Long)

val mesosTaskStats = spark.sql("SELECT state, count(*) AS count FROM mesos_tasks GROUP BY state").
  as[MesosTaskState].
  collect

// TODO - pods!!!
