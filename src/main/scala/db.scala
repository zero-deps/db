package zd.db

import java.io.{File, RandomAccessFile}
import java.nio.file.{Files, Paths}

class Db {
  var main: RandomAccessFile = null
  def open(): Unit = {
    Files.createDirectories(Paths.get("dat"))
    main = new RandomAccessFile("dat/main.db", "rw")
  }
  def close(): Unit = {
    if (main != null) main.close()
  }
}

object Main extends App {
  val db = new Db()
  db.open()
  db.close()
}