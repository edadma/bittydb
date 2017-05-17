package xyz.hyperreal.bittydb

import collection.mutable.WrappedArray


abstract class Blob extends Iterable[Byte]

class ArrayBlob( a: Array[Byte] ) extends Blob {
	private val wrapped = new WrappedArray.ofByte( a )

	def iterator = wrapped.iterator

	override def size = a.length
}

class IOBlob( io: IO, addr: Long, override val size: Int ) extends Blob {
	def iterator = io.byteIterator( addr, size )
}