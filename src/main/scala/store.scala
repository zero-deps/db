package db

import zio._, duration._, stream._
import zio.IO.{effect, effectTotal}
import zero.ext._, option._
import proto._, api.MessageCodec, macros._
import org.rocksdb.{util=>_,_}
import java.util.Arrays

object Store {
  trait Service {
    def put(key: Key, v: Dat): UIO[Unit]
    def get(key: Key): UIO[Option[Dat]]

    def add(fid: Fid, dat: Dat): UIO[Eid]
    def get(fid: Fid, eid: Eid): IO[NotExists, Dat]
    def all(fid: Fid): UStream[Dat]

    def close(): UIO[Unit]
  }

  def live(dir: String): ULayer[Store] =
    ZLayer.fromAcquireRelease {
      for {
        _  <- effectTotal(RocksDB.loadLibrary())
        wo <- effectTotal(WriteOptions())
        ro <- effectTotal(ReadOptions())
        db <- for {
                op <- effectTotal {
                        Options()
                          .setCreateIfMissing(true)
                          .setCompressionType(CompressionType.LZ4_COMPRESSION)
                      }
                x  <- effect(OptimisticTransactionDB.open(op, dir)).map(_.toOption).orDie
                y  <- ZIO.getOrFailWith(Exception("open"))(x).orDie
              } yield y
      } yield new Service {

        def put(key: Key, v: Dat): UIO[Unit] =
          db.pute(key, v)

        def get(key: Key): UIO[Option[Dat]] =
          db.getOrNone(key)

        def add(fid: Fid, dat: Dat): UIO[Eid] =
          db.txe(wo).use { case tx =>
            for {
              l  <- tx.gete(fid, ro).fold({
                      case NotExists => none
                    }, _.some)
              l1 <- l.inc
              _  <- tx.pute(fid, l1)
              _  <- for {
                      k <- encode(Tuple2(fid, l1))
                      v <- encode(Tuple1(l))
                      _ <- tx.pute(k, v)
                    } yield unit
              _  <- for {
                      k <- encode(Tuple3(fid, l1, "dat"))
                      _ <- tx.pute(k, dat)
                    } yield unit
              _  <- tx.commite()
            } yield l1
          }

        def get(fid: Fid, eid: Eid): IO[NotExists, Dat] =
          for {
            k  <- encode(Tuple3(fid, eid, "dat"))
            x  <- db.gete(k)
          } yield x

        def all(fid: Fid): UStream[Dat] =
          entries(fid).mapM(get(fid, _).orDieWith(x => Throwable(x.toString)))

        private def entries(fid: Fid): UStream[Dat] =
          Stream.fromEffect(
            db.gete(fid).fold({case NotExists=>none}, _.some)
          ).flatMap{
              Stream.unfoldM(_){
                case None => IO.succeed(none)
                case Some(eid) =>
                  for {
                    k <- encode(Tuple2(fid, eid))
                    v <- db.gete(k).orDieWith(x => Throwable(x.toString))
                    s <- decode[Tuple1[Option[Eid]]](v)
                  } yield (eid, s._1).some
              }
          }

        given MessageCodec[Tuple1[Option[Eid]]] = caseCodecIdx
        given MessageCodec[Tuple2[Fid,Eid]] = caseCodecIdx
        given MessageCodec[Tuple3[Fid,Eid,String]] = caseCodecIdx

        def close(): UIO[Unit] = db.eff_close()
      }
    }(_.close())
}

def put(key: Key, v: Dat): URIO[Store, Unit] =
  ZIO.accessM(_.get.put(key, v))

def get(key: Key): URIO[Store, Option[Dat]] =
  ZIO.accessM(_.get.get(key))

def add(fid: Fid, dat: Dat): URIO[Store, Eid] =
  ZIO.accessM(_.get.add(fid, dat))

def get(fid: Fid, id: Eid): ZIO[Store, NotExists, Dat] =
  ZIO.accessM(_.get.get(fid, id))

def all(fid: Fid): ZStream[Store, Nothing, Dat] =
  ZStream.accessStream(_.get.all(fid))

type Store = Has[Store.Service]

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
      case None => IO.succeed(Array(Byte.MinValue))
      case Some(x) =>
        for {
          len <- effectTotal(x.length)
          last <- effectTotal(x.last)
          xs <- if last < Byte.MaxValue
                then
                  for {
                    x1 <- effectTotal(Arrays.copyOf(x, len).nn)
                    _  <- effectTotal(x1(len-1) = (last + 1).toByte)
                  } yield x1
                else
                  for {
                    x1 <- effectTotal(Arrays.copyOf(x, len+1).nn)
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
  def pute(k: Array[Byte], v: Array[Byte]): UIO[Unit] =
    effect(db.put(k, v)).orDie
  def gete(k: Array[Byte]): IO[NotExists, Array[Byte]] =
    IO.require(NotExists)(getOrNone(k))
  def getOrNone(k: Array[Byte]): UIO[Option[Array[Byte]]] =
    effect(db.get(k)).map(_.toOption).orDie
  def txe(wo: WriteOptions): UManaged[Transaction] =
    ZManaged.fromAutoCloseable(IO.effect(db.beginTransaction(wo).nn).orDie)
  def eff_close(): UIO[Unit] =
    effectTotal(db.close())

extension (tx: Transaction)
  def pute(k: Array[Byte], v: Array[Byte]): UIO[Unit] =
    effect(tx.put(k, v)).orDie
  def gete(k: Array[Byte], ro: ReadOptions): IO[NotExists, Array[Byte]] =
    IO.require(NotExists)(effect(tx.getForUpdate(ro, k, true)).map(_.toOption).orDie)
  def commite(): UIO[Unit] =
    effect(tx.commit()).orDie

def decode[A](xs: Array[Byte])(using c: MessageCodec[A]): UIO[A] =
  effect(api.decode(xs)).orDie

def encode[A](x: A)(using c: MessageCodec[A]): UIO[Array[Byte]] =
  effectTotal(api.encode(x))

given CanEqual[None.type, Option[?]] = CanEqual.derived
