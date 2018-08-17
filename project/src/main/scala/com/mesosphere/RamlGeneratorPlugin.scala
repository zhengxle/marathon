package mesosphere.raml

import java.io.InputStream

import io.mesosphere.types.gen.backend.impl.StringTemplateBackend
import io.mesosphere.types.gen.backend.impl.FileBackendConsumer
import io.mesosphere.types.gen.backend.BackendConfig
import io.mesosphere.types.gen.frontend.impl.RAMLFrontend
import io.mesosphere.types.gen.frontend.FrontendConfig
import io.mesosphere.types.gen.model.namespaces.Namespace
import sbt.{Def, _}
import sbt.Keys._

object RamlGeneratorPlugin extends AutoPlugin {
  object autoImport {
    lazy val ramlDirectory = settingKey[File]("Base directory where the RAML types are defined in")
    lazy val ramlGenerate = taskKey[Seq[File]]("Generate the RAML files")
  }
  import autoImport._
  override lazy val projectSettings: Seq[Def.Setting[_]] = inConfig(Compile)(Seq(
    ramlDirectory := {
      baseDirectory.value / "docs" / "docs" / "rest-api" / "public" / "api" / "v2" / "types"
    },
    ramlGenerate := {
      generateDTG(ramlDirectory.value, sourceManaged.value, streams.value.log)
    }
  ))

  /**
    * Generate source code from RAML types using the DC/OS Type Generator
    *
    * This function uses the `RAMLFrontend` to parse the RAML types from the given
    * directory and then uses the `StringTemplateBackend` to generate the source code.
    *
    * We are using a custom consumer (the FileBackendConsumerWithTracking) to also
    * keep track of the files we created.
    *
    * @param ramlDirectory The directory where to look for .raml files that contain type definitions
    * @param outputDir The output directory where to create the scala files
    * @param log The logger object to use for reporting status
    * @return Returns a list of the files generated
    */
  def generateDTG(ramlDirectory: File, outputDir: File, log: Logger): Seq[File] = {

    // Create the RAML front-end
    var frontendConfig = new FrontendConfig(ramlDirectory.getAbsolutePath)
    val frontend = new RAMLFrontend()

    // Create and configure the StringTemplate back-end
    val backendConfig = new BackendConfig(outputDir.getAbsolutePath)
    val backend = new StringTemplateBackend(backendConfig)
    backendConfig.setOption("templates", "/Users/icharala/Develop/dcos-type-generator/example/template-scala-2")

    // Use a custom back-end consumer
    val backendConsumer = new FileBackendConsumerWithTracking(backendConfig)

    // Load front-end types and generate the respective files
    var rootNamespace = Namespace.root()
    log.info("Generating RAML types")
    frontend.loadProjectTypes(frontendConfig, rootNamespace)
    backend.generateScope(backendConfig, rootNamespace, backendConsumer)

    // Return the files generated
    backendConsumer.generatedFiles
  }
}

/**
  * A thin extension to the default `FileBackendConsumer` that also keeps track of the files generated
  * @param config The Back-end configuration to use
  */
class FileBackendConsumerWithTracking(config: BackendConfig) extends FileBackendConsumer(config) {
  var generatedFiles: Seq[File] = Seq.empty

  override def collect(contents: InputStream, name: String): Unit = {
    generatedFiles :+ config.targetPath + "/" + name
    super.collect(contents, name)
  }
}
