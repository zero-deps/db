package db

import org.rocksdb.*
import zio.*

case object NotExists
type NotExists = NotExists.type

extension (db: OptimisticTransactionDB)
  def putE(k: Array[Byte], v: Array[Byte]): UIO[Unit] =
    ZIO.attempt(db.put(k, v)).orDie

  def putE(ch: ColumnFamilyHandle, k: Array[Byte], v: Array[Byte]): UIO[Unit] =
    ZIO.attempt(db.put(ch, k, v)).orDie

  def getE(k: Array[Byte]): IO[NotExists, Array[Byte]] =
    getOrNoneE(k).someOrFail(NotExists)

  def getE(ch: ColumnFamilyHandle, k: Array[Byte]): IO[NotExists, Array[Byte]] =
    getOrNoneE(ch, k).someOrFail(NotExists)

  def getOrNoneE(k: Array[Byte]): UIO[Option[Array[Byte]]] =
    ZIO.attempt(db.get(k)).map(_.option).orDie

  def getOrNoneE(ch: ColumnFamilyHandle, k: Array[Byte]): UIO[Option[Array[Byte]]] =
    ZIO.attempt(db.get(ch, k)).map(_.option).orDie

  def txE(wo: WriteOptions): ZIO[Scope, Nothing, Transaction] =
    ZIO.fromAutoCloseable(ZIO.attemptBlocking(db.beginTransaction(wo).nn).orDie)

extension (tx: Transaction)
  def putE(k: Array[Byte], v: Array[Byte]): UIO[Unit] =
    ZIO.attempt(tx.put(k, v)).orDie

  def getE(k: Array[Byte], ro: ReadOptions): IO[NotExists, Array[Byte]] =
    getOrNoneE(k, ro).someOrFail(NotExists)

  def getOrNoneE(k: Array[Byte], ro: ReadOptions): UIO[Option[Array[Byte]]] =
    ZIO.attempt(tx.getForUpdate(ro, k, true)).map(_.option).orDie

  def commitE(): UIO[Unit] =
    ZIO.attempt(tx.commit()).orDie

extension [A](x: A | Null)
  inline def option: Option[A] =
    given [A]: CanEqual[A, A | Null] = CanEqual.derived
    if x == null then None else Some(x)
