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
    def get(key: Key): RIO[ZEnv, Option[Bytes]]
  }

  val live: URLayer[ZEnv, Store] =
    ZLayer.fromEffect {
      for {
        _  <- IO.effectTotal(RocksDB.loadLibrary())
        op <- IO.effectTotal {
                  new Options()
                    .setCreateIfMissing(true)
                    .setCompressionType(CompressionType.LZ4_COMPRESSION)
                }
        db <- IO.effectTotal(RocksDB.open(op, "data")) //todo: zio-config
      } yield new Service {
        def get(key: Key): RIO[ZEnv, Option[Bytes]] =
          for {
            x  <- withRetryOnce(bs => IO.effect(db.get(bs.unsafeArray)), key)
            b  <- if x == null
                    then IO.succeed(none)
                    else IO.effectTotal(Bytes.unsafeWrap(x).some)
          } yield b

        private def withRetryOnce[A](op: Bytes => Task[A], key: Key): RIO[ZEnv, A] =
          for {
            k <- IO.effectTotal(encodeToBytes(key))
            x <- op(k).retry(Schedule.fromDuration(100 milliseconds))
          } yield x

        private given MessageCodec[Tuple2[Bytes,Bytes]] = caseCodecIdx
      }
    }
}

def get(fd: Bytes, id: Bytes): RIO[Store with ZEnv, Option[Bytes]] =
  ZIO.accessM(_.get.get(fd -> id))
