package io.github.edadma

import java.time.{Instant, OffsetDateTime}

package object bittydb {
//	val logger = LoggerFactory.getLogger( classOf[Connection] )

  val FORMAT_VERSION = "1"

  val logging = true

  def log(o: AnyRef): Unit = {
    if (logging)
      println("[log]  " + o.toString)
  }

  def timestamp: Instant = Instant.now

  def datetime: OffsetDateTime = OffsetDateTime.now

  def bitCeiling(n: Long) = {
    val highest = java.lang.Long.highestOneBit(n)

    if ((highest ^ n) == 0)
      highest
    else
      highest << 1
  }
}
