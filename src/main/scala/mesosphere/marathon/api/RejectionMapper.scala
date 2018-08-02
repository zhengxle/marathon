package mesosphere.marathon
package api

import javax.ws.rs.core.Response.Status
import javax.ws.rs.core.Response

object RejectionMapper {
  def rejectionResponse(rejection: Rejection): Response = rejection match {
    case Rejection.AccessDeniedRejection(authorizer, identity) =>
      ResponseFacade(authorizer.handleNotAuthorized(identity, _))

    case Rejection.NotAuthenticatedRejection(authenticator, request) =>
      val requestWrapper = new RequestFacade(request)
      ResponseFacade(authenticator.handleNotAuthenticated(requestWrapper, _))

    case Rejection.ServiceUnavailableRejection =>
      Response.status(Status.SERVICE_UNAVAILABLE).build()

    case Rejection.PathNotFoundRejection(id, version) =>
      Response
        .status(Status.NOT_FOUND)
        .entity(s"Path '$id' does not exist" + version.fold("")(v => s" in version $v"))
        .build()
  }
}
