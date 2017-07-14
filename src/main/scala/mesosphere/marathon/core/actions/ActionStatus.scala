package mesosphere.marathon
package core.actions

object ActionStatus extends Enumeration {
  val Waiting, Running, Succeeded, Failed = Value
}