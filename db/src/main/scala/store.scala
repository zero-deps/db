package db

import zio.*, duration.*, stream.*
import zio.IO.{succeed, effect, effectTotal}
import proto.*
import org.rocksdb.{util as _,*}
import java.util.Arrays
import collection.JavaConverters.*
import util.{*, given}

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

  def live(dir: String, columns: List[Column]=nil): ULayer[Store] =
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
          cHandles.asScala.view.map(x => IArray.unsafeFromArray(x.getName.nn) -> x).toMap
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
              case None => succeed(none)
              case Some(eid) =>
                next(fid, eid).map(n => (eid, n).some)
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
  ZStream.accessStream(_.get.all(fid, after=none))

def all(fid: Fid, after: Eid): ZStream[Store, Nothing, Dat] =
  ZStream.accessStream(_.get.all(fid, after.some))

type Store = Has[Store.Service]

opaque type Column = IArray[Byte]

object Column:
  def apply(xs: IArray[Byte]): Column = xs

opaque type Key = IArray[Byte]

object Key:
  def apply(xs: IArray[Byte]): Key = xs

opaque type Fid = IArray[Byte]

object Fid:
  def apply(xs: IArray[Byte]): Fid = xs

opaque type Eid = IArray[Byte]

extension (x: Option[Eid])
  def inc: UIO[Eid] =
    x match
      case None => succeed(IArray.unsafeFromArray(Array(Byte.MinValue)))
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
              } yield IArray.unsafeFromArray(x1)
            else
              for {
                x1 <- effectTotal(Arrays.copyOf(x.toArray, len+1).nn)
                _  <- effectTotal(x1(len) = Byte.MinValue)
              } yield IArray.unsafeFromArray(x1)
        } yield xs

opaque type Dat = IArray[Byte]

object Dat:
  def apply(xs: IArray[Byte]): Dat = xs

extension (x: Dat)
  def show: String = x.hex.utf8
  def toKey: Key = x
  def bytes: IArray[Byte] = x

case object NotExists
type NotExists = NotExists.type

extension (db: OptimisticTransactionDB)
  def putE(k: IArray[Byte], v: IArray[Byte]): UIO[Unit] =
    effect(db.put(k.toArray, v.toArray)).orDie

  def putE(ch: ColumnFamilyHandle, k: IArray[Byte], v: IArray[Byte]): UIO[Unit] =
    effect(db.put(ch, k.toArray, v.toArray)).orDie

  def getE(k: IArray[Byte]): IO[NotExists, IArray[Byte]] =
    IO.require(NotExists)(getOrNoneE(k))

  def getE(ch: ColumnFamilyHandle, k: IArray[Byte]): IO[NotExists, IArray[Byte]] =
    IO.require(NotExists)(getOrNoneE(ch, k))

  def getOrNoneE(k: IArray[Byte]): UIO[Option[IArray[Byte]]] =
    effect(db.get(k.toArray)).map(_.toOption.map(IArray.unsafeFromArray)).orDie

  def getOrNoneE(ch: ColumnFamilyHandle, k: IArray[Byte]): UIO[Option[IArray[Byte]]] =
    effect(db.get(ch, k.toArray)).map(_.toOption.map(IArray.unsafeFromArray)).orDie

  def txE(wo: WriteOptions): UManaged[Transaction] =
    ZManaged.fromAutoCloseable(IO.effect(db.beginTransaction(wo).nn).orDie)

extension (tx: Transaction)
  def putE(k: IArray[Byte], v: IArray[Byte]): UIO[Unit] =
    effect(tx.put(k.toArray, v.toArray)).orDie

  def getE(k: IArray[Byte], ro: ReadOptions): IO[NotExists, IArray[Byte]] =
    IO.require(NotExists)(getOrNoneE(k, ro))

  def getOrNoneE(k: IArray[Byte], ro: ReadOptions): UIO[Option[IArray[Byte]]] =
    effect(tx.getForUpdate(ro, k.toArray, true)).map(_.toOption.map(IArray.unsafeFromArray)).orDie

  def commitE(): UIO[Unit] =
    effect(tx.commit()).orDie

def decodeE[A](xs: IArray[Byte])(using c: MessageCodec[A]): UIO[A] =
  effect(decodeI(xs)).orDie

def encodeE[A](x: A)(using c: MessageCodec[A]): UIO[IArray[Byte]] =
  effectTotal(encodeI(x))

given CanEqual[None.type, Option[?]] = CanEqual.derived
