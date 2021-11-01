package io.github.edadma.bittydb

import java.io.{RandomAccessFile, File}

class DiskIO(f: File) extends IO {
  private lazy val file = new RandomAccessFile(f, "rw")

  def close: Unit = file.close()

  def force: Unit = file.getChannel.force(true)

  def readLock(addr: Long): Unit = todo

  def writeLock(addr: Long): Unit = todo

  def readUnlock(addr: Long): Unit = todo

  def writeUnlock(addr: Long): Unit = todo

  def size: Long = file.length

  def size_=(l: Long): Unit = file.setLength(l)

  def pos: Long = file.getFilePointer

  def pos_=(p: Long): Unit = file.seek(p)

  def append: Long = {
    val l = file.length

    file.seek(l)
    l
  }

  def getByte: Int = file.readByte

  def putByte(b: Int): Unit = file.writeByte(b)

  def getBytes(len: Int): Array[Byte] = {
    val res = new Array[Byte](len)

    file.readFully(res)
    res
  }

  def putBytes(a: Array[Byte]): Unit = file.write(a)

  def getUnsignedByte: Int = file.readUnsignedByte

  def getChar: Char = file.readChar

  def putChar(c: Char): Unit = file.writeChar(c)

  def getShort: Int = file.readShort

  def putShort(s: Int): Unit = file.writeShort(s)

  def getUnsignedShort: Int = file.readUnsignedShort

  def getInt: Int = file.readInt

  def putInt(i: Int): Unit = file.writeInt(i)

  def getLong: Long = file.readLong

  def putLong(l: Long): Unit = file.writeLong(l)

  def getDouble: Double = file.readDouble

  def putDouble(d: Double): Unit = file.writeDouble(d)

  def writeByteChars(s: String): Unit = file.writeBytes(s)

  def writeBuffer(io: MemIO): Unit = {
    if (io.size > Int.MaxValue)
      sys.error("too big")

    file.write(io.buf.buffer.array, 0, io.buf.size)
  }

  override def toString: String = f.toString
}
