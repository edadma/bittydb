package xyz.hyperreal.bittydb

import java.io.{ByteArrayInputStream, InputStream}


abstract class Blob {
	def length: Long
	def stream: InputStream
}

class ArrayBlob( a: Array[Byte] ) extends Blob {
	val stream = new ByteArrayInputStream( a )
	val length = a.length
}

class IOBlob( io: IO, addr: Long, val length: Long ) extends Blob {
	def stream = io.byteInputStream( addr, length )
}