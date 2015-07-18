package ca.hyperreal.bittydb

import java.util.concurrent.locks.ReentrantReadWriteLock

import collection.concurrent.TrieMap


class MemIO extends IO
{
	private [bittydb] lazy val db = new ExpandableByteBuffer
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
	
	def size: Long = db.size
	
	def size_=( l: Long ) = db.size = l.asInstanceOf[Int]
	
	def pos: Long = db.buffer.position
	
	def pos_=( p: Long ) {
		assert( p <= size, "file pointer must be less than or equal to file size" )
		db.buffer.position( p.asInstanceOf[Int] )
	}
	
	def append: Long = {
		pos = size
		size
	}
	
	def getByte: Int = {
		db.getting( 1 )
		db.buffer.get
	}
	
	def putByte( b: Int ) {
		db.putting( 1 )
		db.buffer.put( b.asInstanceOf[Byte] )
	}
	
	def getBytes( len: Int ): Array[Byte] = {
		db.getting( len )
		
		val res = new Array[Byte]( len )
		
		db.buffer.get( res )
		res
	}
	
	def putBytes( a: Array[Byte] ) = {
		db.putting( a.length )
		db.buffer.put( a )
	}
	
	def getUnsignedByte: Int = getByte&0xFF
	
	def getChar: Char = {
		db.getting( 2 )
		db.buffer.getChar
	}
	
	def putChar( c: Char ) {
		db.putting( 2 )
		db.buffer.putChar( c )
	}
	
	def getShort: Int = {
		db.getting( 2 )
		db.buffer.getShort
	}
	
	def putShort( s: Int ) {
		db.putting( 2 )
		db.buffer.putShort( s.asInstanceOf[Short] )
	}
	
	def getUnsignedShort: Int = getShort&0xFFFF
	
	def getInt: Int = {
		db.getting( 4 )
		db.buffer.getInt
	}
	
	def putInt( i: Int ) {
		db.putting( 4 )
		db.buffer.putInt( i )
	}
	
	def getLong: Long = {
		db.getting( 8 )
		db.buffer.getLong
	}
	
	def putLong( l: Long ) {
		db.putting( 8 )
		db.buffer.putLong( l )
	}
	
	def getDouble: Double = {
		db.getting( 8 )
		db.buffer.getDouble
	}
	
	def putDouble( d: Double ) {
		db.putting( 8 )
		db.buffer.putDouble( d )
	}

	def writeByteChars( s: String ) = s foreach {c => putByte( c.asInstanceOf[Int] )}
	
	def writeBuffer( buf: MemIO ) {
		if (buf.size > Int.MaxValue)
			sys.error( "too big" )
			
		val len = buf.size.asInstanceOf[Int]
		
		db.putting( len )
		db.buffer.put( buf.db.buffer.array, 0, len )
	}
	
	override def toString = "mem"
}