package mesosphere.marathon
package json

import java.util.concurrent.TimeUnit

import com.fasterxml.jackson.core.JsonGenerator
import com.fasterxml.jackson.databind.module.SimpleModule
import com.fasterxml.jackson.databind.{ ObjectMapper, SerializerProvider }
import com.fasterxml.jackson.databind.ser.std.StdSerializer
import mesosphere.marathon.raml.{ GroupConversion, Raml }
import mesosphere.marathon.state.{ AppDefinition, Group, RootGroup, Timestamp }
import org.openjdk.jmh.annotations._
import org.openjdk.jmh.infra.Blackhole
import play.api.libs.json.{ JsValue, Json }

@State(Scope.Benchmark)
class JsonSerializeDeserializeState {

  @Param(value = Array("real/155_100.json", "real/155_200.json", "real/155_500.json", "real/155_800.json", "real/155_1000.json"))
  var jsonMockFile: String = _

  /**
    * The contents of the JSON mock file as string (for de-serialisation)
    */
  lazy val jsonMockContents: String = {
    import java.io.InputStream
    val is: InputStream = getClass.getResourceAsStream(s"/mocks/json/${jsonMockFile}")
    scala.io.Source.fromInputStream(is).mkString
  }

  /**
    * The contents of the JSON mock file as a de-serialised RAML object (for serialisation)
    */
  lazy val groupMock: raml.Group = {
    val value: JsValue = Json.parse(jsonMockContents)
    Json.fromJson[raml.Group](value).get
  }

  /**
    * The contents of the JSON mock file as an updatable root group
    */
  lazy val rootGroupMock: Group = {
    import mesosphere.marathon.raml.GroupConversion._
    val value: JsValue = Json.parse(jsonMockContents)
    val groupUpdate: raml.GroupUpdate = Json.fromJson[raml.GroupUpdate](value).get

    val group: RootGroup = RootGroup()
    val appConversionFunc: (raml.App => AppDefinition) = Raml.fromRaml[raml.App, AppDefinition]

    Raml.fromRaml(
      GroupConversion(groupUpdate, group, Timestamp.zero) -> appConversionFunc)
  }

  class UnreachableStrategySerializer extends StdSerializer[raml.UnreachableStrategy](classOf[raml.UnreachableStrategy]) {

    override def serialize(value: raml.UnreachableStrategy, gen: JsonGenerator, provider: SerializerProvider): Unit = {
      gen.writeStartObject()

      value match {
        case raml.UnreachableDisabled(v) => gen.writeString(v)
        case raml.UnreachableEnabled(inactiveAfterSeconds, expungeAfterSeconds) =>
          gen.writeNumberField("inactiveAfterSeconds", inactiveAfterSeconds)
          gen.writeNumberField("expungeAfterSeconds", expungeAfterSeconds)
      }

      gen.writeEndObject()
    }
  }

  class VersionInfoSerializer extends StdSerializer[raml.VersionInfo](classOf[raml.VersionInfo]) {

    override def serialize(value: raml.VersionInfo, gen: JsonGenerator, provider: SerializerProvider): Unit = {
      gen.writeStartObject()

      gen.writeStringField("lastScalingAt", value.lastScalingAt.toString)
      gen.writeStringField("lastConfigChangeAt", value.lastConfigChangeAt.toString)

      gen.writeEndObject()
    }
  }

  class UpgradeStrategySerializer extends StdSerializer[raml.UpgradeStrategy](classOf[raml.UpgradeStrategy]) {

    override def serialize(value: raml.UpgradeStrategy, gen: JsonGenerator, provider: SerializerProvider): Unit = {
      gen.writeStartObject()

      gen.writeNumberField("maximumOverCapacity", value.maximumOverCapacity)
      gen.writeNumberField("minimumHealthCapacity", value.minimumHealthCapacity)

      gen.writeEndObject()
    }
  }

  class PortDefinitionSerializer extends StdSerializer[raml.PortDefinition](classOf[raml.PortDefinition]) {

    override def serialize(value: raml.PortDefinition, gen: JsonGenerator, provider: SerializerProvider): Unit = {
      gen.writeStartObject()

      gen.writeNumberField("port", value.port)

      gen.writeObjectFieldStart("labels")
      value.labels.foreach {
        case (key, value) =>
          gen.writeStringField(key, value)
      }
      gen.writeEndObject()

      value.name.foreach(gen.writeStringField("name", _))
      gen.writeStringField("protocol", value.protocol.value)

      gen.writeEndObject()
    }
  }
  class NetworkSerializer extends StdSerializer[raml.Network](classOf[raml.Network]) {

    override def serialize(value: raml.Network, gen: JsonGenerator, provider: SerializerProvider): Unit = {
      gen.writeStartObject()

      value.name.foreach(gen.writeStringField("name", _))
      gen.writeStringField("mode", value.mode.value)

      gen.writeObjectFieldStart("labels")
      value.labels.foreach {
        case (key, value) =>
          gen.writeStringField(key, value)
      }
      gen.writeEndObject()

      gen.writeEndObject()
    }
  }
  class DockerSerializer extends StdSerializer[raml.DockerContainer](classOf[raml.DockerContainer]) {

    override def serialize(value: raml.DockerContainer, gen: JsonGenerator, provider: SerializerProvider): Unit = {
      gen.writeStartObject()

      gen.writeStringField("fill", "me")

      gen.writeEndObject()
    }
  }

  class ContainerSerializer extends StdSerializer[raml.Container](classOf[raml.Container]) {

    override def serialize(value: raml.Container, gen: JsonGenerator, provider: SerializerProvider): Unit = {
      gen.writeStartObject()

      gen.writeStringField("type", value.`type`.value)
      value.docker.foreach { docker =>
        gen.writeObjectField("docker", docker)
      }
      //val appc = play.api.libs.json.Json.toJson(o.appc)
      //val volumes = play.api.libs.json.Json.toJson(o.volumes)
      //val portMappings = play.api.libs.json.Json.toJson(o.portMappings)

      gen.writeEndObject()
    }
  }

  class AppSerializer() extends StdSerializer[raml.App](classOf[raml.App]) {

    override def serialize(value: raml.App, gen: JsonGenerator, provider: SerializerProvider): Unit = {
      gen.writeStartObject()

      gen.writeStringField("id", value.id)

      value.acceptedResourceRoles.foreach { roles =>
        gen.writeArrayFieldStart("acceptedResourceRoles")
        roles.foreach { role => gen.writeString(role) }
        gen.writeEndArray()
      }

      // TODO(karsten): handle null or non empty
      gen.writeArrayFieldStart("args")
      value.args.foreach{ arg => gen.writeString(arg) }
      gen.writeEndArray()

      gen.writeNumberField("backoffFactor", value.backoffFactor)
      gen.writeNumberField("backoffSeconds", value.backoffSeconds)

      value.cmd.foreach { cmd =>
        gen.writeStringField("cmd", cmd)
      }

      // TODO(karsten): handle null or non empty
      gen.writeArrayFieldStart("constraints")
      value.constraints.foreach { constraint =>
        gen.writeStartArray()
        constraint.foreach(gen.writeString(_))
        gen.writeEndArray()
      }
      gen.writeEndArray()

      // TODO(karsten): write container serializer
      value.container.foreach { container =>
        gen.writeObjectField("container", container)
      }

      gen.writeNumberField("cpus", value.cpus)

      // TODO(karsten): handle null or non empty
      gen.writeArrayFieldStart("dependencies")
      value.dependencies.foreach { dependency =>
        gen.writeString(dependency)
      }
      gen.writeEndArray()

      gen.writeNumberField("disk", value.disk)

      value.env.foreach {
        case (name, raml.EnvVarValue(v)) => gen.writeStringField(name, v)
        case (name, raml.EnvVarSecret(v)) =>
          gen.writeObjectFieldStart(name)
          gen.writeStringField("secret", v)
          gen.writeEndObject()
      }

      gen.writeStringField("executor", value.executor)

      // TODO(karsten): handle null or non empty
      value.fetch.foreach { f =>
        // TODO: write
      }

      gen.writeArrayFieldStart("healthChecks")
      value.healthChecks.foreach { check =>
        // TODO: write
      }
      gen.writeEndArray()

      gen.writeNumberField("instances", value.instances)

      gen.writeObjectFieldStart("labels")
      value.labels.foreach {
        case (key, value) =>
          gen.writeStringField(key, value)
      }
      gen.writeEndObject()

      gen.writeNumberField("maxLaunchDelaySeconds", value.maxLaunchDelaySeconds)

      gen.writeNumberField("mem", value.mem)
      gen.writeNumberField("gpus", value.gpus)

      value.ipAddress.foreach { ip =>
        // TODO
        //gen.writeObjectField("ipAddress", ip)
      }

      gen.writeArrayFieldStart("networks")
      value.networks.foreach { network =>
        gen.writeObject(network)
      }
      gen.writeEndArray()

      value.ports.foreach { ports =>
        gen.writeArrayFieldStart("ports")
        ports.foreach(gen.writeNumber(_))
        gen.writeEndArray()
      }

      value.portDefinitions.foreach { portDefinitions =>
        gen.writeArrayFieldStart("portDefinitions")
        portDefinitions.foreach(gen.writeObject(_))
        gen.writeEndArray()
      }

      value.readinessChecks.foreach { readinessCheck =>
        // TODO: write
      }

      value.residency.foreach { residency =>
        // TODO: write
      }

      gen.writeBooleanField("requirePorts", value.requirePorts)

      value.secrets.foreach {
        case (key, value) =>
        // TODO: write
      }

      value.taskKillGracePeriodSeconds.foreach { taskKillGracePeriod =>
        gen.writeNumberField("taskKillGracePeriodSeconds", taskKillGracePeriod)
      }

      value.upgradeStrategy.foreach { strategy =>
        gen.writeObjectField("upgradeStrategy", strategy)
      }

      value.uris.foreach { uris =>
        gen.writeArrayFieldStart("uris")
        uris.foreach(gen.writeString(_))
        gen.writeEndArray()
      }

      value.user.foreach { user =>
        gen.writeStringField("user", user)
      }

      value.version.foreach { version =>
        gen.writeStringField("version", version.toString)
      }

      value.versionInfo.foreach { versionInfo =>
        gen.writeObjectField("versionInfo", versionInfo)
      }

      gen.writeStringField("killSelection", value.killSelection.value)

      value.upgradeStrategy.foreach { strategy =>
        gen.writeObjectField("unreachableStrategy", strategy)
      }

      value.tty.foreach { tty =>
        gen.writeBooleanField("tty", tty)
      }

      gen.writeEndObject()
    }
  }

  class GroupSerializer() extends StdSerializer[raml.Group](classOf[raml.Group]) {

    override def serialize(value: raml.Group, gen: JsonGenerator, provider: SerializerProvider): Unit = {
      gen.writeStartObject()
      gen.writeStringField("id", value.id)

      gen.writeArrayFieldStart("apps")
      // write apps
      value.apps.foreach { app =>
        gen.writeObject(app)
      }
      gen.writeEndArray()

      gen.writeObjectField("pods", Array.empty)
      gen.writeObjectField("groups", Array.empty)
      gen.writeObjectField("dependencies", Array.empty)
      gen.writeStringField("version", value.version.toString)
      gen.writeEndObject()
    }
  }

}

@OutputTimeUnit(TimeUnit.MILLISECONDS)
@BenchmarkMode(Array(Mode.AverageTime))
@Fork(1)
class JsonSerializeDeserializeBenchmark extends JsonSerializeDeserializeState {

  @Benchmark
  def jsonParse(hole: Blackhole): Unit = {
    val value: JsValue = Json.parse(jsonMockContents)
    hole.consume(value)
  }

  @Benchmark
  def jsonParseDeserialise(hole: Blackhole): Unit = {
    val value: JsValue = Json.parse(jsonMockContents)
    val groupUpdate: raml.GroupUpdate = Json.fromJson[raml.GroupUpdate](value).get
    hole.consume(groupUpdate)
  }

  @Benchmark
  def jsonParseDeserialiseUpdate(hole: Blackhole): Unit = {
    import mesosphere.marathon.raml.GroupConversion._
    val value: JsValue = Json.parse(jsonMockContents)
    val groupUpdate: raml.GroupUpdate = Json.fromJson[raml.GroupUpdate](value).get

    val group: RootGroup = RootGroup()
    val appConversionFunc: (raml.App => AppDefinition) = Raml.fromRaml[raml.App, AppDefinition]
    val updatedGroup: Group = Raml.fromRaml(
      GroupConversion(groupUpdate, rootGroupMock, Timestamp.now()) -> appConversionFunc)

    hole.consume(updatedGroup)
  }

  @Benchmark
  def jsonDeserialise(hole: Blackhole): Unit = {
    val value: JsValue = Json.toJson[raml.Group](groupMock)
    hole.consume(value)
  }

  @Benchmark
  def jsonSerialiseWrite(hole: Blackhole): Unit = {
    val value: JsValue = Json.toJson[raml.Group](groupMock)
    val str: String = value.toString()
    hole.consume(str)
  }

  @Benchmark
  def jsonSerialiseWriteNew(hole: Blackhole): Unit = {
    val mapper = new ObjectMapper()
    val module = new SimpleModule()
    module.addSerializer(classOf[raml.Group], new GroupSerializer())
    module.addSerializer(classOf[raml.App], new AppSerializer())
    module.addSerializer(classOf[raml.Container], new ContainerSerializer())
    module.addSerializer(classOf[raml.DockerContainer], new DockerSerializer())
    module.addSerializer(classOf[raml.Network], new NetworkSerializer())
    module.addSerializer(classOf[raml.PortDefinition], new PortDefinitionSerializer())
    module.addSerializer(classOf[raml.VersionInfo], new VersionInfoSerializer())
    mapper.registerModule(module)

    val ser = mapper.writeValueAsString(groupMock)
  }

}