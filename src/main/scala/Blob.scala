package xyz.hyperreal.bittydb

import java.io.{ByteArrayInputStream, DataInputStream, InputStream}


abstract class Blob {
	def length: Long

	def stream: InputStream

	def bytes( offset: Long, length: Int ) = {
		val res = new Array[Byte]( length )
		val s = new DataInputStream( stream )

		s.skip( offset )
		s.readFully( res )
		s.close
		res
	}
}

class ArrayBlob( a: Array[Byte] ) extends Blob {
	val length = a.length

	def stream = new ByteArrayInputStream( a )

	override def bytes( offset: Long, length: Int ) = a.slice( offset.toInt, offset.toInt + length )
}

class IOBlob( io: IO, addr: Long, val length: Long ) extends Blob {
	def stream = io.byteInputStream( addr, length )
}