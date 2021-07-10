package db

import zio.*, test.*, Assertion.*

object StoreSpec extends DefaultRunnableSpec {
  def spec = suite("StoreSpec")(
    testM("feed") {
      val fid = Fid(Array(5))
      val dat1 = Dat(Array('a'))
      val dat2 = Dat(Array('b'))
      for {
        _  <- add(fid, dat1)
        _  <- add(fid, dat2)
        xs <- all(fid).runCollect
      } yield assert(xs.map(_.show))(equalTo(Chunk(dat2.show, dat1.show)))
    }
  ).provideLayerShared(Store.live(s"target/${System.nanoTime}"))
}
