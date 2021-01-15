package db

import zio._

object DbApp extends App {
  def run(args: List[String]): URIO[ZEnv, ExitCode] =
    for {
      _ <- IO.succeed(1)
    } yield ExitCode.success
}
