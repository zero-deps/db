package db

import scala.annotation.*
import zio.*

opaque type Column = Array[Byte]

object Column:
  def apply(xs: Array[Byte]): Column = xs
end Column

extension (x: Column)
  @targetName("arrForColumn")
  def arr: Array[Byte] = x
end extension

opaque type Key = Array[Byte]

object Key:
  def apply(xs: Array[Byte]): Key = xs
end Key

extension (x: Key)
  @targetName("arrForKey")
  def arr: Array[Byte] = x
end extension

opaque type Fid = Array[Byte]

object Fid:
  def apply(xs: Array[Byte]): Fid = xs
end Fid

extension (x: Fid)
  @targetName("arrForFid")
  def arr: Array[Byte] = x
end extension

opaque type Eid = Array[Byte]

object Eid:
  def apply(x: Array[Byte]): Eid = x
  val default: Eid = Array(Byte.MinValue)
end Eid

extension (x: Eid)
  @targetName("arrForEid")
  def arr: Array[Byte] = x

  def inc: UIO[Eid] =
    for
      len <- ZIO.succeed(x.length)
      last <- ZIO.succeed(x.last)
      xs <-
        if last < Byte.MaxValue
        then
          for
            x1 <- ZIO.attempt(java.util.Arrays.copyOf(x, len).nn).orDie
            _  <- ZIO.succeed(x1(len-1) = (last + 1).toByte)
          yield x1
        else
          for
            x1 <- ZIO.attempt(java.util.Arrays.copyOf(x, len+1).nn).orDie
            _  <- ZIO.succeed(x1(len) = Byte.MinValue)
          yield x1
    yield xs
end extension

opaque type Dat = Array[Byte]

object Dat:
  def apply(xs: Array[Byte]): Dat = xs
end Dat

extension (x: Dat)
  def toKey: Key = x
  @targetName("arrForDat")
  def arr: Array[Byte] = x
end extension
