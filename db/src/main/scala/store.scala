package db

import zio.*, duration.*, stream.*
import zio.IO.{succeed, effect, effectTotal}
import proto.*
import org.rocksdb.{util as _,*}
import java.util.Arrays
import collection.JavaConverters.*
import util.{*, given}

import ext.*

object Store {
  trait Service {
    def put(key: Key, v: Dat): UIO[Unit]
    def get(key: Key): UIO[Option[Dat]]

    def put(column: Column, key: Key, v: Dat): UIO[Unit]
    def get(column: Column, key: Key): UIO[Option[Dat]]

    def add(fid: Fid, dat: Dat): UIO[Eid]
    def all(fid: Fid, after: Option[Eid]): UStream[Dat]

    def close(): UIO[Unit]
  }

  def live(dir: String, columns: List[Column]=Nil): ULayer[Store] =
    ZLayer.fromAcquireRelease {
      for {
        _  <- effectTotal(RocksDB.loadLibrary())
        wo <- effectTotal(WriteOptions())
        ro <- effectTotal(ReadOptions())
        co <- effectTotal(ColumnFamilyOptions().optimizeUniversalStyleCompaction())
        cDescriptors <-
          effectTotal(
            (RocksDB.DEFAULT_COLUMN_FAMILY :: columns.map(_.toArray))
              .map(ColumnFamilyDescriptor(_, co))
              .asJava
          )
        cHandles <- succeed(new java.util.ArrayList[ColumnFamilyHandle])
        db <- for {
                op <-
                  effectTotal {
                    DBOptions().nn
                      .setCreateIfMissing(true).nn
                      .setCreateMissingColumnFamilies(true).nn
                  }
                y <-
                  effect(
                    OptimisticTransactionDB.open(op, dir, cDescriptors, cHandles).nn
                  ).orDie
              } yield y
      } yield new Service {

        def put(key: Key, v: Dat): UIO[Unit] =
          db.putE(key, v)

        def get(key: Key): UIO[Option[Dat]] =
          db.getOrNoneE(key)

        private val cHandleMap =
          cHandles.asScala.view.map(x => x.getName.nn -> x).toMap
        private def getCHandle(column: Column): UIO[ColumnFamilyHandle] = 
          effect(cHandleMap(column)).orDie

        def put(column: Column, key: Key, v: Dat): UIO[Unit] =
          for {
            ch <- getCHandle(column)
            r <- db.putE(ch, key, v)
          } yield r

        def get(column: Column, key: Key): UIO[Option[Dat]] =
          for {
            ch <- getCHandle(column)
            r <- db.getOrNoneE(ch, key)
          } yield r

        def add(fid: Fid, dat: Dat): UIO[Eid] =
          db.txE(wo).use { case tx =>
            for {
              l  <- tx.getOrNoneE(fid, ro)
              l1 <- l.inc
              _  <- tx.putE(fid, l1)
              _  <- for {
                      k <- encodeE(Tuple2(fid, l1))
                      v <- encodeE(Tuple1(l))
                      _ <- tx.putE(k, v)
                    } yield unit
              _  <- for {
                      k <- encodeE(Tuple3(fid, l1, "dat"))
                      _ <- tx.putE(k, dat)
                    } yield unit
              _  <- tx.commitE()
            } yield l1
          }

        def all(fid: Fid, after: Option[Eid]): UStream[Dat] =
          entries(fid, after).mapM(eid => (for {
            k <- encodeE(Tuple3(fid, eid, "dat"))
            x <- db.getE(k)
          } yield x).orDieWith(x => Throwable(x.toString)))

        private def entries(fid: Fid, after: Option[Eid]): UStream[Dat] =
          Stream.fromEffect{
            val n: UIO[Option[Eid]] =
              after match
                case None => db.getOrNoneE(fid)
                case Some(eid) => next(fid, eid)
            n
          }.flatMap{
            Stream.unfoldM(_){
              case None => succeed(None)
              case Some(eid) =>
                next(fid, eid).map(n => Some((eid, n)))
            }
          }

        private def next(fid: Fid, eid: Eid): UIO[Option[Eid]] =
          for {
            k <- encodeE(Tuple2(fid, eid))
            v <- db.getE(k).orDieWith(x => Throwable(x.toString))
            s <- decodeE[Tuple1[Option[Eid]]](v)
          } yield s._1

        given MessageCodec[Tuple1[Option[Eid]]] = caseCodecIdx
        given MessageCodec[Tuple2[Fid,Eid]] = caseCodecIdx
        given MessageCodec[Tuple3[Fid,Eid,String]] = caseCodecIdx

        def close(): UIO[Unit] =
          effect(
            cHandles.asScala.foreach(_.close())
          ).tapBoth(
            _ => effect(db.close())
          , _ => effect(db.close())
          )
            .orDie
      }
    }(_.close())
}

def put(key: Key, v: Dat): URIO[Store, Unit] =
  ZIO.accessM(_.get.put(key, v))

def get(key: Key): URIO[Store, Option[Dat]] =
  ZIO.accessM(_.get.get(key))

def put(column: Column, key: Key, v: Dat): URIO[Store, Unit] =
  ZIO.accessM(_.get.put(key, v))

def get(column: Column, key: Key): URIO[Store, Option[Dat]] =
  ZIO.accessM(_.get.get(key))

def add(fid: Fid, dat: Dat): URIO[Store, Eid] =
  ZIO.accessM(_.get.add(fid, dat))

def all(fid: Fid): ZStream[Store, Nothing, Dat] =
  ZStream.accessStream(_.get.all(fid, after=None))

def all(fid: Fid, after: Eid): ZStream[Store, Nothing, Dat] =
  ZStream.accessStream(_.get.all(fid, Some(after)))

type Store = Has[Store.Service]

opaque type Column = Array[Byte]

object Column:
  def apply(xs: Array[Byte]): Column = xs

opaque type Key = Array[Byte]

object Key:
  def apply(xs: Array[Byte]): Key = xs

opaque type Fid = Array[Byte]

object Fid:
  def apply(xs: Array[Byte]): Fid = xs

opaque type Eid = Array[Byte]

extension (x: Option[Eid])
  def inc: UIO[Eid] =
    x match
      case None => succeed(Array(Byte.MinValue))
      case Some(x) =>
        for {
          len <- effectTotal(x.length)
          last <- effectTotal(x.last)
          xs <-
            if last < Byte.MaxValue
            then
              for {
                x1 <- effectTotal(Arrays.copyOf(x.toArray, len).nn)
                _  <- effectTotal(x1(len-1) = (last + 1).toByte)
              } yield x1
            else
              for {
                x1 <- effectTotal(Arrays.copyOf(x.toArray, len+1).nn)
                _  <- effectTotal(x1(len) = Byte.MinValue)
              } yield x1
        } yield xs

opaque type Dat = Array[Byte]

object Dat:
  def apply(xs: Array[Byte]): Dat = xs

extension (x: Dat)
  def show: String = x.hex.utf8
  def toKey: Key = x
  def bytes: Array[Byte] = x

case object NotExists
type NotExists = NotExists.type

extension (db: OptimisticTransactionDB)
  def putE(k: Array[Byte], v: Array[Byte]): UIO[Unit] =
    effect(db.put(k.toArray, v.toArray)).orDie

  def putE(ch: ColumnFamilyHandle, k: Array[Byte], v: Array[Byte]): UIO[Unit] =
    effect(db.put(ch, k.toArray, v.toArray)).orDie

  def getE(k: Array[Byte]): IO[NotExists, Array[Byte]] =
    IO.require(NotExists)(getOrNoneE(k))

  def getE(ch: ColumnFamilyHandle, k: Array[Byte]): IO[NotExists, Array[Byte]] =
    IO.require(NotExists)(getOrNoneE(ch, k))

  def getOrNoneE(k: Array[Byte]): UIO[Option[Array[Byte]]] =
    effect(db.get(k.toArray)).map(_.option).orDie

  def getOrNoneE(ch: ColumnFamilyHandle, k: Array[Byte]): UIO[Option[Array[Byte]]] =
    effect(db.get(ch, k.toArray)).map(_.option).orDie

  def txE(wo: WriteOptions): UManaged[Transaction] =
    ZManaged.fromAutoCloseable(IO.effect(db.beginTransaction(wo).nn).orDie)

extension (tx: Transaction)
  def putE(k: Array[Byte], v: Array[Byte]): UIO[Unit] =
    effect(tx.put(k.toArray, v.toArray)).orDie

  def getE(k: Array[Byte], ro: ReadOptions): IO[NotExists, Array[Byte]] =
    IO.require(NotExists)(getOrNoneE(k, ro))

  def getOrNoneE(k: Array[Byte], ro: ReadOptions): UIO[Option[Array[Byte]]] =
    effect(tx.getForUpdate(ro, k.toArray, true)).map(_.option).orDie

  def commitE(): UIO[Unit] =
    effect(tx.commit()).orDie

def decodeE[A](xs: Array[Byte])(using c: MessageCodec[A]): UIO[A] =
  effect(decode(xs)).orDie

def encodeE[A](x: A)(using c: MessageCodec[A]): UIO[Array[Byte]] =
  effectTotal(encode(x))

given CanEqual[None.type, Option[?]] = CanEqual.derived
