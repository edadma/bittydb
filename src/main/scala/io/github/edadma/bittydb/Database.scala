package io.github.edadma.bittydb

import java.io.{RandomAccessFile, File}

object Database {
  def exists(file: String): Boolean = exists(new File(file))

  def exists(f: File): Boolean = f.exists

  def isAccessible(file: String): Boolean = isAccessible(new File(file))

  def isAccessible(f: File): Boolean = f.canRead && f.canWrite

  def remove(file: String): Unit = { remove(new File(file)) }

  def remove(f: File): Unit = { f.delete }

  def isValid(file: String): Boolean = isValid(new File(file))

  def isValid(f: File): Boolean = {
    if (exists(f))
      try {
        Connection.disk(f).close()
        true
      } catch {
        case e: InvalidDatabaseException => false
      } else
      sys.error("non-existent")
  }
}
