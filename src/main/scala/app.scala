package db
import store._

import zio._, console._
import zd.proto._

object DbApp extends App {
  def run(args: List[String]): URIO[ZEnv, ExitCode] =
    val fd = Bytes.unsafeWrap(Array(1))
    val id = Bytes.unsafeWrap(Array(1))
    val data = Bytes.unsafeWrap(Array('a'))
    (for {
      _ <- store.put(fd, id, data)
      x <- store.get(fd, id)
      _ <- putStrLn(x.map(_.mkString).toString)
    } yield ()).provideCustomLayer(Store.live("data")).exitCode
}
