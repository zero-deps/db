package db

import zio._, console._
import zd.proto._
import zero.ext._, option._

object DbApp extends App {
  def run(args: List[String]): URIO[ZEnv, ExitCode] =
    val fid = Fid(Array(1))
    val dat = Dat(Array('a'))
    (for {
      id <- add(fid, dat)
      x  <- get(fid, id)
      _  <- putStrLn(x.mkString)
    } yield ()).provideCustomLayer(Store.live("data")).exitCode
}
