package db
package store

import zio._, duration._
import zero.ext._, option._
import zd.proto._, api._, macrosapi._
import org.rocksdb.{util=>_,_}

type Store = Has[Store.Service]
type Key = Tuple2[Bytes, Bytes]

object Store {
  trait Service {
    def put(key: Key, data: Bytes): RIO[ZEnv, Unit]
    def get(key: Key             ): RIO[ZEnv, Option[Bytes]]
    def close(): UIO[Unit]
  }

  def live(dir: String): URLayer[ZEnv, Store] =
    ZLayer.fromAcquireRelease {
      for {
        _  <- IO.effectTotal(RocksDB.loadLibrary())
        op <- IO.effectTotal {
                  new Options()
                    .setCreateIfMissing(true)
                    .setCompressionType(CompressionType.LZ4_COMPRESSION)
                }
        db <- IO.effectTotal(RocksDB.open(op, dir))
      } yield new Service {
        def get(key: Key): RIO[ZEnv, Option[Bytes]] =
          for {
            x  <- withRetryOnce(bs => IO.effect(db.get(bs.unsafeArray)), key)
            b  <- if x == null
                    then IO.succeed(none)
                    else IO.effectTotal(Bytes.unsafeWrap(x).some)
          } yield b          

        def put(key: Key, data: Bytes): RIO[ZEnv, Unit] =
          withRetryOnce(bs => IO.effect(db.put(bs.unsafeArray, data.unsafeArray)), key)

        private def withRetryOnce[A](op: Bytes => Task[A], key: Key): RIO[ZEnv, A] =
          for {
            k <- IO.effectTotal(encodeToBytes(key))
            x <- op(k).retry(Schedule.fromDuration(100 milliseconds))
          } yield x

        private given MessageCodec[Tuple2[Bytes,Bytes]] = caseCodecIdx

        def close(): UIO[Unit] = IO.effectTotal(db.close())
      }
    }(_.close())
}

def get(fd: Bytes, id: Bytes): RIO[Store with ZEnv, Option[Bytes]] =
  ZIO.accessM(_.get.get(fd -> id))

def put(fd: Bytes, id: Bytes, data: Bytes): RIO[Store with ZEnv, Unit] =
  ZIO.accessM(_.get.put(fd -> id, data))
