package db

import zio.*

case class Conf(dir: String, columns: List[Column])

object Conf:
  val layer: ZLayer[System, Nothing, Conf] =
    ZLayer:
      for
        sys <- ZIO.service[System]
        dir <- sys.env("DB_DIR").orDie.someOrElseZIO(ZIO.dieMessage("None"))
        columns <- sys.env("DB_COLUMNS").orDie
        parsedColumns <- columns.fold(ZIO.succeed(Nil))(parse)
      yield Conf(dir, parsedColumns)

  private def parse(x: String): UIO[List[Column]] =
    ZIO.attempt(x.split(",").nn.toList.map(x => Column(x.nn.trim.nn.getBytes.nn))).orDie
end Conf
