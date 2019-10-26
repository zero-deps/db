package db

import zio.*, test.*, Assertion.*

object StoreSpec extends ZIOSpecDefault:
  def spec = suite("StoreSpec")(
    test("feed"):
      val fid = Fid(Array(5))
      val dat1 = Dat(Array('a'))
      val dat2 = Dat(Array('b'))
      for
        _ <- add(fid, dat1)
        _ <- add(fid, dat2)
        xs <- all(fid).runCollect
      yield assert(xs.map(_.arr.hex))(equalTo(Chunk(dat2.arr.hex, dat1.arr.hex)))
  ).provideLayerShared(
    TestSystem.live(
      TestSystem.Data(
        envs = Map("DB_DIR" -> s"target/${java.lang.System.nanoTime}")
      )
    ) >>>
    Conf.layer >>>
    StoreImpl.layer
  )

extension (x: Array[Byte])
  def hex: String =
    val acc = new Array[Char](x.length * 2)
    var i = 0
    while (i < x.length)
      val v = x(i) & 0xff
      acc(i * 2) = hexs(v >>> 4)
      acc(i * 2 + 1) = hexs(v & 0x0f)
      i += 1
    String(acc)

val hexs = ('0' to '9') ++ ('a' to 'f')
