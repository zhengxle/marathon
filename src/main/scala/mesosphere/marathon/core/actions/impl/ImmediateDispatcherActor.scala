package mesosphere.marathon
package core.actions.impl

import akka.actor.{ Actor, ActorLogging, Props }
import mesosphere.marathon.core.actions.impl.ImmediateDispatcherActor.DestroyInstance
import mesosphere.marathon.core.instance.Instance
import mesosphere.marathon.core.task.termination.{ KillReason, KillService }

private[actions] class ImmediateDispatcherActor(
    killService: KillService) extends Actor with ActorLogging {
  def receive: Receive = {
    case DestroyInstance(instance, killReason) => killService.killInstance(instance, killReason)
  }
}

object ImmediateDispatcherActor {
  def props(killService: KillService): Props = Props(new ImmediateDispatcherActor(killService))

  case class DestroyInstance(instance: Instance, killReason: KillReason)
}