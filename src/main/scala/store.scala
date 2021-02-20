package db

import zio._, duration._, stream._
import zio.IO.{effect, effectTotal}
import zero.ext._, option._
import zd.proto.{Bytes=>_,_}, api.MessageCodec, macrosapi._
import org.rocksdb.{util=>_,_}
import java.util.Arrays

object Store {
  trait Service {
    def add(fid: Fid, dat: Dat): UIO[Eid]
    def get(fid: Fid, eid: Eid): IO[NotExists, Dat]
    def all(fid: Fid): UStream[Dat]
    def close(): UIO[Unit]
  }

  def live(dir: String): ULayer[Store] =
    ZLayer.fromAcquireRelease {
      for {
        _  <- effect(RocksDB.loadLibrary()).orDie
        wo <- effect(WriteOptions()).orDie //?
        ro <- effect(ReadOptions()).orDie //?
        db <- for {
                op <- effect {
                        Options()
                          .setCreateIfMissing(true)
                          .setCompressionType(CompressionType.LZ4_COMPRESSION)
                      }.orDie //?
                x  <- effect(OptimisticTransactionDB.open(op, dir)).map(_.toOption).orDie
                y  <- ZIO.getOrFailWith(new Exception("open"))(x).orDie
              } yield y
      } yield new Service {

        def add(fid: Fid, dat: Dat): UIO[Eid] =
          db.eff_tx(wo).use { case tx =>
            for {
              l  <- tx.eff_get(fid, ro).fold({
                      case NotExists => none
                    }, _.some)
              l1 <- l.inc
              _  <- tx.eff_put(fid, l1)
              _  <- for {
                      k <- Tuple2(fid, l1).encode
                      v <- Tuple1(l).encode
                      _ <- tx.eff_put(k, v)
                    } yield unit
              _  <- for {
                      k <- Tuple3(fid, l1, "dat").encode
                      _ <- tx.eff_put(k, dat)
                    } yield unit
              _  <- tx.eff_commit()
            } yield l1
          }

        def get(fid: Fid, eid: Eid): IO[NotExists, Dat] =
          for {
            k  <- Tuple3(fid, eid, "dat").encode
            x  <- db.eff_get(k)
          } yield x

        def all(fid: Fid): UStream[Dat] =
          entries(fid).mapM(get(fid, _).orDieWith(x => Throwable(x.toString)))

        private def entries(fid: Fid): UStream[Dat] =
          Stream.fromEffect(
            db.eff_get(fid).fold({case NotExists=>none}, _.some)
          ).flatMap{
              Stream.unfoldM(_){
                case None => IO.succeed(none)
                case Some(eid) =>
                  for {
                    k <- Tuple2(fid, eid).encode
                    v <- db.eff_get(k).orDieWith(x => Throwable(x.toString))
                    s <- v.decode[Tuple1[Option[Eid]]]
                  } yield (eid, s._1).some
              }
          }

        private given MessageCodec[Tuple1[Option[Eid]]] = caseCodecIdx
        private given MessageCodec[Tuple2[Fid,Eid]] = caseCodecIdx
        private given MessageCodec[Tuple3[Fid,Eid,String]] = caseCodecIdx

        def close(): UIO[Unit] = db.eff_close()
      }
    }(_.close())
}

def add(fid: Fid, dat: Dat): RIO[Store, Eid] =
  ZIO.accessM(_.get.add(fid, dat))

def get(fid: Fid, id: Eid): ZIO[Store, NotExists, Dat] =
  ZIO.accessM(_.get.get(fid, id))

def all(fid: Fid): ZStream[Store, Nothing, Dat] =
  ZStream.accessStream(_.get.all(fid))

type Store = Has[Store.Service]
type Bytes = Array[Byte]

opaque type Fid = Bytes
object Fid:
  def apply(xs: Bytes): Fid = xs

opaque type Eid = Bytes
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

opaque type Dat = Bytes
object Dat:
  def apply(xs: Bytes): Dat = xs

case object NotExists
type NotExists = NotExists.type

extension (db: OptimisticTransactionDB)
  def eff_put(k: Bytes, v: Bytes): UIO[Unit] =
    effect(db.put(k, v)).orDie
  def eff_get(k: Bytes): IO[NotExists, Bytes] =
    IO.require(NotExists)(effect(db.get(k)).map(_.toOption).orDie)
  def eff_tx(wo: WriteOptions): UManaged[Transaction] =
    ZManaged.fromAutoCloseable(IO.effect(db.beginTransaction(wo).nn).orDie)
  def eff_close(): UIO[Unit] =
    effectTotal(db.close())

extension (tx: Transaction)
  def eff_put(k: Bytes, v: Bytes): UIO[Unit] =
    effect(tx.put(k, v)).orDie
  def eff_get(k: Bytes, ro: ReadOptions): IO[NotExists, Bytes] =
    IO.require(NotExists)(effect(tx.getForUpdate(ro, k, true)).map(_.toOption).orDie)
  def eff_commit(): UIO[Unit] =
    effect(tx.commit()).orDie

extension [A](xs: Array[Byte])
  def decode(using c: MessageCodec[A]): UIO[A] =
    effect(api.decode(xs)).orDie

extension [A](x: A)
  def encode(using c: MessageCodec[A]): UIO[Bytes] =
    effectTotal(api.encode(x))

given CanEqual[None.type, Option[?]] = CanEqual.derived
