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
      } yield assert(x.hex)(equalTo(dat.hex)) &&
              assert(xs.map(hex))(equalTo(Chunk(dat2.hex, dat.hex)))
    }
  ).provideLayerShared(Store.live("target/testdata"))
}
