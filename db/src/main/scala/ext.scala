package db

extension (x: Array[Byte])
  def hex: Array[Byte] =
    val acc = new Array[Byte](x.length * 2)
    var i = 0
    while (i < x.length) {
      val v = x(i) & 0xff
      acc(i * 2) = hexs(v >>> 4)
      acc(i * 2 + 1) = hexs(v & 0x0f)
      i += 1
    }
    acc

  inline def utf8: String =
    String(x.toArray, "utf8")

private val hexs = "0123456789abcdef".getBytes("ascii").nn

inline def unit: Unit = ()

extension [A](x: A | Null)
  inline def option: Option[A] = if x == null then None else Some(x)

given [A]: CanEqual[A, A | Null] = CanEqual.derived
