package db

import zio.*

import proto.*

given MessageCodec[Tuple1[Option[Array[Byte]]]] = caseCodecIdx
given MessageCodec[Tuple2[Array[Byte],Array[Byte]]] = caseCodecIdx
given MessageCodec[Tuple3[Array[Byte],Array[Byte],String]] = caseCodecIdx

def decodeE[A](xs: Array[Byte])(using c: MessageCodec[A]): UIO[A] =
  ZIO.attempt(decode(xs)).orDie

def encodeE[A](x: A)(using c: MessageCodec[A]): UIO[Array[Byte]] =
  ZIO.succeed(encode(x))
