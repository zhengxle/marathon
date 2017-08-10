package mesosphere.marathon
package api.v2

import java.util.UUID
import javax.inject.Inject
import javax.servlet.http.HttpServletRequest
import javax.ws.rs._
import javax.ws.rs.core.{ Context, MediaType, Response }

import akka.event.EventStream
import mesosphere.marathon.api.{ AuthResource, MarathonMediaType, RestResource }
import mesosphere.marathon.core.actions.ActionManager
import mesosphere.marathon.core.actions.impl.ActionManagerActor
import mesosphere.marathon.core.actions.impl.actions.{ CreateInstanceAction, DestroyInstanceAction }
import mesosphere.marathon.core.instance.Instance
import mesosphere.marathon.core.task.termination.KillReason
import mesosphere.marathon.plugin.auth._
import mesosphere.marathon.state.PathId

@Path("v2/actions")
@Consumes(Array(MediaType.APPLICATION_JSON))
@Produces(Array(MarathonMediaType.PREFERRED_APPLICATION_JSON))
class ActionsResource @Inject() (
    eventBus: EventStream,
    val config: MarathonConf,
    actionManager: ActionManager)(implicit
  val authenticator: Authenticator,
    val authorizer: Authorizer) extends RestResource with AuthResource {

  @GET
  def index(
    @Context req: HttpServletRequest): Response = authenticated(req) { implicit identity =>
    Response.ok("this should list all actions").build()
  }

  @DELETE
  @Path("""instances/{id:.+}""")
  def destroyInstance(
    @PathParam("id") id: String,
    @Context req: HttpServletRequest): Response = authenticated(req) { implicit identity =>
    val action = DestroyInstanceAction(Instance.Id(id), KillReason.KillingTasksViaApi, UUID.randomUUID())
    val actionResult = actionManager.add(action)
    val response = actionResult match {
      case _: ActionManagerActor.Result.InstanceNotFound => Response.status(404).entity("unknown instance id")
      case _: ActionManagerActor.Result.Success => Response.ok("action created")
      case _ => Response.status(400).entity("Unknown action manager result")
    }
    response.build()
  }

  @POST
  @Path("""instances/{runSpecId:.+}""")
  def createInstance(
    @PathParam("runSpecId") id: String,
    @Context req: HttpServletRequest): Response = authenticated(req) { implicit identity =>
    val action = CreateInstanceAction(PathId.fromSafePath(id), UUID.randomUUID())
    val actionResult = actionManager.add(action)
    val response = actionResult match {
      case _: ActionManagerActor.Result.RunSpecNotFound => Response.status(404).entity("unknown run spec id")
      case _: ActionManagerActor.Result.Success => Response.ok("action created")
      case _ => Response.status(400).entity("Unknown action manager result")
    }
    response.build()
  }
}