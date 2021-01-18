package db

import zio._, console._
import zd.proto._
import zero.ext._, option._

//todo: move to test
object DbApp extends App {
  def run(args: List[String]): URIO[ZEnv, ExitCode] =
    val fid = Fid(Array(2))
    val dat = Dat(Array('a'))
    val dat2 = Dat(Array('b'))
    (for {
      id <- add(fid, dat)
      x  <- get(fid, id)
      _  <- putStrLn(x.mkString)
      _  <- add(fid, dat2)
      xs <- all(fid).runCollect
      _  <- xs.mapM(x => putStrLn(x.mkString))
    } yield ()).provideCustomLayer(Store.live("data")).exitCode
}
