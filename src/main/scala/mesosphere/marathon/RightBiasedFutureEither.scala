package mesosphere.marathon

import scala.concurrent.{ExecutionContext, Future}

object RightBiasedFutureEither {
  implicit class FutureEither[+L, R](wrapped: Future[Either[L, R]]) {
    def right = this
    def unwrap = wrapped
    def map[RR](f: R => RR)(implicit ec: ExecutionContext): FutureEither[L, RR] =
      new FutureEither(wrapped.map(_.map(f)))

    def flatMap[LL >: L, RR](f: R => FutureEither[LL, RR])(implicit ec: ExecutionContext): FutureEither[LL, RR] = new FutureEither(
      wrapped.flatMap {
        case Left(s) => Future.successful(Left(s))
        case Right(a) => f(a).unwrap
      }
    )
  }

  implicit class FutureNothingEither[T](wrapped: Future[T]) {
    def asRight(implicit ec: ExecutionContext): FutureEither[Nothing, T] =
      new FutureEither(wrapped.map(Right(_)))
  }
}
