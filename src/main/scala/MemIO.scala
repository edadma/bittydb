package xyz.hyperreal.bittydb

import java.util.concurrent.locks.ReentrantReadWriteLock

import collection.concurrent.TrieMap


class MemIO extends IO
{
	private [bittydb] lazy val buf = new ExpandableByteBuffer( maxsize min Int.MaxValue toInt )
	private [bittydb] val locks = new TrieMap[Long, ReentrantReadWriteLock]

	def lock( addr: Long ) =
		locks get addr match {
			case Some( l ) => l
			case None =>
				val nl = new ReentrantReadWriteLock

				locks.putIfAbsent( addr, nl ) match {
					case Some( l ) => l
					case None => nl
				}
		}

	def close {}

	def force {}

	def readLock( addr: Long ) = lock( addr ).readLock.lock

	def writeLock( addr: Long ) = lock( addr ).writeLock.lock

	def readUnlock( addr: Long ) = lock( addr ).readLock.unlock

	def writeUnlock( addr: Long ) = lock( addr ).writeLock.unlock

	def size: Long = buf.size
	
	def size_=( l: Long ) = buf.size = l.asInstanceOf[Int]
	
	def pos: Long = buf.buffer.position
	
	def pos_=( p: Long ) {
		assert( p <= size, "file pointer must be less than or equal to file size" )
		buf.buffer.position( p.asInstanceOf[Int] )
	}
	
	def append: Long = {
		pos = size
		size
	}
	
	def getByte: Int = {
		buf.getting( 1 )
		buf.buffer.get
	}
	
	def putByte( b: Int ) {
		buf.putting( 1 )
		buf.buffer.put( b.asInstanceOf[Byte] )
	}
	
	def getBytes( len: Int ): Array[Byte] = {
		buf.getting( len )
		
		val res = new Array[Byte]( len )
		
		buf.buffer.get( res )
		res
	}
	
	def putBytes( a: Array[Byte] ) {
		buf.putting( a.length )
		buf.buffer.put( a )
	}

	def putBytes( a: Array[Byte], offset: Int, length: Int ): Unit = {
		buf.putting( a.length )
		buf.buffer.put( a, offset, length )
	}

	def getUnsignedByte: Int = getByte&0xFF
	
	def getChar: Char = {
		buf.getting( 2 )
		buf.buffer.getChar
	}
	
	def putChar( c: Char ) {
		buf.putting( 2 )
		buf.buffer.putChar( c )
	}
	
	def getShort: Int = {
		buf.getting( 2 )
		buf.buffer.getShort
	}
	
	def putShort( s: Int ) {
		buf.putting( 2 )
		buf.buffer.putShort( s.asInstanceOf[Short] )
	}
	
	def getUnsignedShort: Int = getShort&0xFFFF
	
	def getInt: Int = {
		buf.getting( 4 )
		buf.buffer.getInt
	}
	
	def putInt( i: Int ) {
		buf.putting( 4 )
		buf.buffer.putInt( i )
	}
	
	def getLong: Long = {
		buf.getting( 8 )
		buf.buffer.getLong
	}
	
	def putLong( l: Long ) {
		buf.putting( 8 )
		buf.buffer.putLong( l )
	}
	
	def getDouble: Double = {
		buf.getting( 8 )
		buf.buffer.getDouble
	}
	
	def putDouble( d: Double ) {
		buf.putting( 8 )
		buf.buffer.putDouble( d )
	}

	def writeByteChars( s: String ) = s foreach {c => putByte( c.asInstanceOf[Int] )}
	
	def writeBuffer( io: MemIO ) {
		if (io.size > Int.MaxValue)
			sys.error( "too big" )

		val len = io.size.asInstanceOf[Int]
		
		buf.putting( len )
		buf.buffer.put( io.buf.buffer.array, 0, len )
	}
	
	override def toString = "mem"
}