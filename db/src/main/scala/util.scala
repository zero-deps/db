package db.util

extension (x: IArray[Byte])
  def hex: IArray[Byte] =
    val acc = new Array[Byte](x.length * 2)
    var i = 0
    while (i < x.length) {
      val v = x(i) & 0xff
      acc(i * 2) = hexs(v >>> 4)
      acc(i * 2 + 1) = hexs(v & 0x0f)
      i += 1
    }
    IArray.unsafeFromArray(acc)

  inline def utf8: String =
    String(x.toArray, "utf8")

private val hexs = "0123456789abcdef".getBytes("ascii").nn

inline def unit: Unit = ()
inline def none: None.type = None
inline def nil: Nil.type = Nil

extension [A](x: A)
  inline def some: Some[A] = Some(x)

extension [A](x: A | Null)
  inline def toOption: Option[A] = if x == null then None else Some(x)

given [A]: CanEqual[A, A | Null] = CanEqual.derived
