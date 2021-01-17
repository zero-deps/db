package db

import zio._, duration._, IO.{effect, effectTotal}
import zero.ext._, option._
import zd.proto.{Bytes=>_,_}, api.MessageCodec, macrosapi._
import org.rocksdb.{util=>_,_}
import java.util.Arrays

object Store {
  trait Service {
    def add(fid: Fid, dat: Dat): RIO[ZEnv, Eid]
    // def put(fid: Fid, eid: Eid, dat: Dat): ZIO[ZEnv, Throwable | NotExists, Eid]
    def getOrFail(fid: Fid, eid: Eid): ZIO[ZEnv, Throwable | NotExists, Dat]
    def get(fid: Fid, eid: Eid): RIO[ZEnv, Option[Dat]]
    // def entries(fid: Fid): ZStream[ZEnv, Throwable, Eid]
    // def all(fid: Fid): ZStream[ZEnv, Throwable, Dat] =
    //   entries(fid).mapM(get(fid, _))
    def close(): UIO[Unit]
  }

  def live(dir: String): ZLayer[ZEnv, Throwable, Store] =
    ZLayer.fromAcquireRelease {
      for {
        _  <- effect(RocksDB.loadLibrary())
        wo <- effect(WriteOptions())
        ro <- effect(ReadOptions())
        db <- for {
                op <- effect {
                        Options()
                          .setCreateIfMissing(true)
                          .setCompressionType(CompressionType.LZ4_COMPRESSION)
                      }
                x  <- effect(OptimisticTransactionDB.open(op, dir)).map(_.toOption)
                y  <- ZIO.getOrFailWith(new Exception("open"))(x)
              } yield y
      } yield new Service {

        def get(fid: Fid, eid: Eid): RIO[ZEnv, Option[Dat]] =
          for {
            k  <- Tuple3(fid, eid, "dat").encode
            x  <- db.eff_get(k)
          } yield x

        def getOrFail(fid: Fid, eid: Eid): ZIO[ZEnv, Throwable | NotExists, Dat] =
          for {
            x  <- get(fid, eid)
            x1 <- ZIO.getOrFailWith(NotExists)(x)
          } yield x1

        def add(fid: Fid, dat: Dat): RIO[ZEnv, Eid] =
          for {
            tx <- db.eff_tx(wo)
            l  <- tx.eff_get(fid, ro)
            l1  = l.inc
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

        // def put(fid: Fid, eid: Eid, dat: Dat): ZIO[ZEnv, Throwable | NotExists, Eid] =
        //   for {
        //     tx <- effect(db.beginTransaction(wo))
        //     _  <- IO.
        //   }

        private given MessageCodec[Tuple1[Option[Eid]]] = caseCodecIdx
        private given MessageCodec[Tuple2[Fid,Eid]] = caseCodecIdx
        private given MessageCodec[Tuple3[Fid,Eid,String]] = caseCodecIdx

        def close(): UIO[Unit] = db.eff_close()
      }
    }(_.close())
}

type ZStore = Store with ZEnv

// def all(fid: Fid): ZStream[ZStore, Throwable, Dat] =
//   ZIO.accessM(_.get.all(fid))

def get(fid: Fid, id: Eid): RIO[ZStore, Option[Dat]] =
  ZIO.accessM(_.get.get(fid, id))

def getOrFail(fid: Fid, id: Eid): ZIO[ZStore, Throwable | NotExists, Dat] =
  ZIO.accessM(_.get.getOrFail(fid, id))

def add(fid: Fid, dat: Dat): RIO[ZStore, Eid] =
  ZIO.accessM(_.get.add(fid, dat))

// def put(fid: Fid, id: Eid, dat: Dat): ZIO[ZStore, Throwable | NotExists, Unit] =
//   ZIO.accessM(_.get.put(fid, id, dat))

type Store = Has[Store.Service]
type Bytes = Array[Byte]

opaque type Fid = Bytes
object Fid:
  def apply(xs: Bytes): Fid = xs

opaque type Eid = Bytes
extension (x: Option[Eid])
  def inc: Eid =
    x match
      case None => Array(Byte.MinValue)
      case Some(x) =>
        val len = x.length
        if x.last < Byte.MaxValue
        then
          val x1 = Arrays.copyOf(x, len).nn
          x1(len-1) = (x.last + 1).toByte
          x1
        else
          val x1 = Arrays.copyOf(x, len+1).nn
          x1(len) = Byte.MinValue
          x1
object Eid:
  val empty: Eid = Array.emptyByteArray
  val min: Eid = Array(Byte.MinValue)

opaque type Dat = Bytes
extension (x: Dat)
  def mkString: String = String(x, "utf8")
object Dat:
  def apply(xs: Bytes): Dat = xs

case object NotExists
type NotExists = NotExists.type

extension (db: OptimisticTransactionDB)
  def eff_put(k: Bytes, v: Bytes): Task[Unit] =
    effect(db.put(k, v))
  def eff_get(k: Bytes): Task[Option[Bytes]] =
    effect(db.get(k)).map(_.toOption)
  def eff_tx(wo: WriteOptions): Task[Transaction] =
    effect(db.beginTransaction(wo).nn)
  def eff_close(): UIO[Unit] =
    effectTotal(db.close())

extension (tx: Transaction)
  def eff_put(k: Bytes, v: Bytes): Task[Unit] =
    effect(tx.put(k, v))
  def eff_get(k: Bytes, ro: ReadOptions): Task[Option[Bytes]] =
    effect(tx.get(ro, k)).map(_.toOption)
  def eff_commit(): Task[Unit] =
    effect(tx.commit())

extension[A] (xs: Array[Byte])
  def decode(using c: MessageCodec[A]): Task[A] =
    effect(api.decode(xs))

extension[A] (x: A)
  def encode(using c: MessageCodec[A]): UIO[Bytes] =
    effectTotal(api.encode(x))
