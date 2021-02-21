package db

import zio._, test._, Assertion._

object StoreSpec extends DefaultRunnableSpec {
  def spec = suite("StoreSpec")(
    testM("basic") {
      val fid = Fid(Array(5))
      val dat = Dat(Array('a'))
      val dat2 = Dat(Array('b'))
      for {
        id <- add(fid, dat)
        x  <- get(fid, id)
        _  <- add(fid, dat2)
        xs <- all(fid).runCollect
      } yield assert(x.show)(equalTo(dat.show)) &&
              assert(xs.map(_.show))(equalTo(Chunk(dat2.show, dat.show)))
    }
  ).provideLayerShared(Store.live(s"target/${System.nanoTime}"))
}
