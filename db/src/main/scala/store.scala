package db

import org.rocksdb.*
import scala.jdk.CollectionConverters.*
import zio.*, stream.*

trait Store:
  def put(key: Key, v: Dat): UIO[Unit]
  def get(key: Key): UIO[Option[Dat]]

  def put(column: Column, key: Key, v: Dat): UIO[Unit]
  def get(column: Column, key: Key): UIO[Option[Dat]]

  def add(fid: Fid, dat: Dat): UIO[Eid]
  def all(fid: Fid, after: Option[Eid]): UStream[Dat]

  def close(): UIO[Unit]
end Store

class StoreImpl(
  db: OptimisticTransactionDB
, cHandles: List[ColumnFamilyHandle]
, wo: WriteOptions
, ro: ReadOptions
) extends Store:
  def put(key: Key, v: Dat): UIO[Unit] =
    db.putE(key.arr, v.arr)

  def get(key: Key): UIO[Option[Dat]] =
    db.getOrNoneE(key.arr).map(_.map(Dat(_)))

  private val cHandleMap =
    cHandles.view.map(x => x.getName.nn -> x).toMap
  private def getCHandle(column: Column): UIO[ColumnFamilyHandle] = 
    ZIO.attempt(cHandleMap(column.arr)).orDie

  def put(column: Column, key: Key, v: Dat): UIO[Unit] =
    for
      ch <- getCHandle(column)
      r <- db.putE(ch, key.arr, v.arr)
    yield r

  def get(column: Column, key: Key): UIO[Option[Dat]] =
    for
      ch <- getCHandle(column)
      r <- db.getOrNoneE(ch, key.arr)
    yield r.map(Dat(_))

  def add(fid: Fid, dat: Dat): UIO[Eid] =
    ZIO.scoped:
      for
        tx <- db.txE(wo)
        l <- tx.getOrNoneE(fid.arr, ro).map(_.map(Eid(_)))
        l1 <- l.fold(ZIO.succeed(Eid.default))(_.inc)
        _  <- tx.putE(fid.arr, l1.arr)
        _  <-
          for
            k <- encodeE(Tuple2(fid.arr, l1.arr))
            v <- encodeE(Tuple1(l.map(_.arr)))
            _ <- tx.putE(k, v)
          yield ZIO.unit
        _  <-
          for
            k <- encodeE(Tuple3(fid.arr, l1.arr, "dat"))
            _ <- tx.putE(k, dat.arr)
          yield ZIO.unit
        _  <- tx.commitE()
      yield l1

  def all(fid: Fid, after: Option[Eid]): UStream[Dat] =
    entries(fid, after).mapZIO(eid => (for
      k <- encodeE(Tuple3(fid.arr, eid.arr, "dat"))
      x <- db.getE(k)
    yield Dat(x)).orDieWith(x => Throwable(x.toString)))

  private def entries(fid: Fid, after: Option[Eid]): UStream[Eid] =
    ZStream
      .fromZIO:
        after match
          case None => db.getOrNoneE(fid.arr).map(_.map(Eid(_)))
          case Some(eid) => next(fid, eid)
      .flatMap:
        ZStream.unfoldZIO(_):
          case None => ZIO.none
          case Some(eid) =>
            next(fid, eid).map(n => Some((eid, n)))

  private def next(fid: Fid, eid: Eid): UIO[Option[Eid]] =
    for
      k <- encodeE(Tuple2(fid.arr, eid.arr))
      v <- db.getE(k).orDieWith(x => new Throwable(x.toString))
      s <- decodeE[Tuple1[Option[Array[Byte]]]](v)
    yield s._1.map(Eid.apply)

  def close(): UIO[Unit] =
    ZIO.attemptBlocking(
      cHandles.foreach(_.close())
    ).tapBoth(
      _ => ZIO.attemptBlocking(db.close()),
      _ => ZIO.attemptBlocking(db.close())
    ).orDie
end StoreImpl

object StoreImpl:
  val layer: ZLayer[Conf, Nothing, Store] =
    ZLayer.scoped:
      for
        conf <- ZIO.service[Conf]
        _  <- ZIO.attemptBlocking(RocksDB.loadLibrary()).orDie
        wo <- ZIO.attemptBlocking(WriteOptions()).orDie
        ro <- ZIO.attemptBlocking(ReadOptions()).orDie
        co <- ZIO.attemptBlocking(ColumnFamilyOptions().optimizeUniversalStyleCompaction()).orDie
        cDescriptors <-
          ZIO.attemptBlocking(
            (Column(RocksDB.DEFAULT_COLUMN_FAMILY.nn) :: conf.columns)
              .map(x => ColumnFamilyDescriptor(x.arr, co))
              .asJava
          ).orDie
        cHandles <- ZIO.succeed(new java.util.ArrayList[ColumnFamilyHandle])
        db <-
          for
            op <-
              ZIO
                .attemptBlocking:
                  DBOptions().nn
                    .setCreateIfMissing(true).nn
                    .setCreateMissingColumnFamilies(true).nn
                .orDie
            y <-
              ZIO.attemptBlocking(
                OptimisticTransactionDB.open(op, conf.dir, cDescriptors, cHandles).nn
              ).orDie
          yield y
        service = StoreImpl(db, cHandles.asScala.toList, wo, ro)
        _ <- ZIO.addFinalizer(service.close())
      yield service
end StoreImpl

def put(key: Key, v: Dat): URIO[Store, Unit] =
  ZIO.serviceWithZIO[Store](_.put(key, v))

def get(key: Key): URIO[Store, Option[Dat]] =
  ZIO.serviceWithZIO[Store](_.get(key))

def put(column: Column, key: Key, v: Dat): URIO[Store, Unit] =
  ZIO.serviceWithZIO[Store](_.put(column, key, v))

def get(column: Column, key: Key): URIO[Store, Option[Dat]] =
  ZIO.serviceWithZIO[Store](_.get(column, key))

def add(fid: Fid, dat: Dat): URIO[Store, Eid] =
  ZIO.serviceWithZIO[Store](_.add(fid, dat))

def all(fid: Fid): ZStream[Store, Nothing, Dat] =
  ZStream.serviceWithStream[Store](_.all(fid, after=None))

def all(fid: Fid, after: Eid): ZStream[Store, Nothing, Dat] =
  ZStream.serviceWithStream[Store](_.all(fid, Some(after)))
