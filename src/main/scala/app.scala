package db
import store._

import zio._, console._
import zd.proto._

object DbApp extends App {
  def run(args: List[String]): URIO[ZEnv, ExitCode] =
    val fd = Bytes.unsafeWrap(Array(1))
    val id = Bytes.unsafeWrap(Array(1))
    (for {
      x <- store.get(fd, id)
      _ <- putStrLn(x.toString)
    } yield ()).provideCustomLayer(Store.live).exitCode
}
