package db

import zio._, duration._
import zero.ext._, option._
import zd.proto._, api.MessageCodec, macrosapi._
import org.rocksdb.{util=>_,_}
import java.util.Arrays

type Store = Has[Store.Service]

opaque type Fid = Array[Byte]
object Fid:
  def apply(xs: Array[Byte]): Fid = xs
end Fid

opaque type Eid = Array[Byte]
extension (x: Eid)
  def inc: Eid =
    if x.isEmpty
    then Array(Byte.MinValue)
    else
      val len = x.length
      if x.last < Byte.MaxValue
      then
        val x1 = Arrays.copyOf(x, len)
        x1(len-1) = (x.last + 1).toByte
        x1
      else
        val x1 = Arrays.copyOf(x, len+1)
        x1(len) = Byte.MinValue
        x1
object Eid:
  def apply(xs: Array[Byte]): Eid = xs
  def empty: Eid = Array.emptyByteArray
end Eid

opaque type Dat = Array[Byte]
extension (x: Dat)
  def mkString: String = String(x, "utf8")
object Dat:
  def apply(xs: Array[Byte]): Dat = xs
end Dat

object Store {
  trait Service {
    def add(fid: Fid, dat: Dat): RIO[ZEnv, Eid]
    def get(fid: Fid, eid: Eid): RIO[ZEnv, Option[Dat]]
    // def all(fid: Fid): ZStream[ZEnv, Throwable, Dat]
    def close(): UIO[Unit]
  }

  def live(dir: String): ZLayer[ZEnv, Throwable, Store] =
    ZLayer.fromAcquireRelease {
      for {
        _  <- IO.effect(RocksDB.loadLibrary())
        op <- IO.effect {
                  Options()
                    .setCreateIfMissing(true)
                    .setCompressionType(CompressionType.LZ4_COMPRESSION)
                }
        wo <- IO.effect(WriteOptions())
        db <- IO.effect(OptimisticTransactionDB.open(op, dir))
      } yield new Service {

        def get(fid: Fid, eid: Eid): RIO[ZEnv, Option[Dat]] =
          for {
            k  <- encode(fid -> eid)
            x  <- IO.effect(db.get(k)).map(a=>fromNullable(a))
          } yield x

        def add(fid: Fid, dat: Dat): RIO[ZEnv, Eid] =
          for {
            tx <- IO.effect(db.beginTransaction(wo))
            l  <- IO.effect(db.get(fid)).map{ xs =>
                    if xs == null
                    then Eid.empty
                    else xs
                  }
            l1  = l.inc
            _  <- IO.effect(db.put(fid, l1))
            k  <- encode(fid -> l1)
            _  <- IO.effect(db.put(k, dat))
            _  <- IO.effect(tx.commit())
          } yield l1

        private given MessageCodec[Tuple2[Fid,Eid]] = caseCodecIdx

        def close(): UIO[Unit] = IO.effectTotal(db.close())
      }
    }(_.close())
}

type ZStore = Store with ZEnv

// def all(fid: Fid): ZStream[ZStore, Throwable, Dat] =
//   ZIO.accessM(_.get.all(fid))

def get(fid: Fid, id: Eid): RIO[ZStore, Option[Dat]] =
  ZIO.accessM(_.get.get(fid, id))

def add(fid: Fid, dat: Dat): RIO[ZStore, Eid] =
  ZIO.accessM(_.get.add(fid, dat))

// def upd(fid: Fid, id: Eid, dat: Dat): ZIO[ZStore, Throwable | NotExists, Unit] =
//   ZIO.accessM(_.get.upd(fid, id, dat))

private def decode[A](xs: Array[Byte])(implicit c: MessageCodec[A]): Task[A] =
  IO.effect(api.decode(xs))

private def encode[A](x: A)(implicit c: MessageCodec[A]): UIO[Array[Byte]] =
  IO.effectTotal(api.encode(x))