package xyz.hyperreal.bittydb

import java.nio.charset.Charset
import java.nio.charset.StandardCharsets.UTF_8
import java.time.{Instant, OffsetDateTime, ZoneOffset}
import java.util.UUID

import collection.AbstractIterator
import collection.mutable.{ListBuffer, HashMap}


object IO {
	private [bittydb] var bwidth_default = 5
	private [bittydb] var cwidth_default = 8
}

abstract class IO extends IOConstants {
	private [bittydb] var charset = UTF_8
	private [bittydb] var bwidth = IO.bwidth_default					// big integer (i.e. pointers, sizes) width (2 minimum)
	private [bittydb] var cwidth = IO.cwidth_default					// cell width
	private [bittydb] var bucketsPtr: Long = _
	private [bittydb] var buckets: Array[Long] = _
	
	private [bittydb] lazy val vwidth = 1 + cwidth					// value width
	private [bittydb] lazy val pwidth = 1 + 2*vwidth 					// pair width
	private [bittydb] lazy val ewidth = 1 + vwidth
	private [bittydb] lazy val lowestSize = bitCeiling( vwidth + 1 ).toInt		// smallest allocation block needed
	private [bittydb] lazy val sizeShift = Integer.numberOfTrailingZeros( lowestSize )
	private [bittydb] lazy val bucketLen = bwidth*8 - sizeShift
	
//	println( bwidth, lowestSize, sizeShift, bucketLen )

	//
	// abstract methods
	//
	
	def close
	
	def force
	
	def readLock( addr: Long )
	
	def writeLock( addr: Long )
	
	def readUnlock( addr: Long )
	
	def writeUnlock( addr: Long )
	
	def size: Long
	
	def size_=( l: Long )
	
	def pos: Long
	
	def pos_=( p: Long )
	
	def append: Long
	
	def getByte: Int
	
	def putByte( b: Int )
	
	def getBytes( len: Int ): Array[Byte]
	
	def putBytes( a: Array[Byte] )
	
	def getUnsignedByte: Int
	
	def getChar: Char
	
	def putChar( c: Char )
	
	def getShort: Int
	
	def putShort( s: Int )
	
	def getUnsignedShort: Int
	
	def getInt: Int
	
	def putInt( i: Int )
	
	def getLong: Long
	
	def putLong( l: Long )
	
	def getDouble: Double
	
	def putDouble( d: Double )
	
	def writeByteChars( s: String )
	
	def writeBuffer( buf: MemIO )

	//
	// i/o methods based on abstract methods
	//

	def getSmall: Int = (getByte << 16) | (getUnsignedByte << 8) | getUnsignedByte

	def putSmall( a: Int ): Unit = {
		putByte( a >> 16 )
		putByte( a >> 8 )
		putByte( a )
	}

	def getBig: Long = {
		var res = 0L
		
		for (_ <- 1 to bwidth) {
			res <<= 8
			res |= getUnsignedByte
		}
		
		res
	}

	def putBig( l: Long ) {
		for (shift <- (bwidth - 1)*8 to 0 by -8)
			putByte( (l >> shift).asInstanceOf[Int] )
	}
	
	def getBig( addr: Long ): Long = {
		pos = addr
		getBig
	}
	
	def putBig( addr: Long, l: Long ) {
		pos = addr
		putBig( l )
	}
	
	def addBig( a: Long ) {
		val cur = pos
		val v = getBig
		
		pos = cur
		putBig( v + a )
	}
	
	def addBig( addr: Long, a: Long ) {
		pos = addr
		addBig( a )
	}
	
	def getUnsignedByte( addr: Long ): Int = {
		pos = addr
		getUnsignedByte
	}
	
	def getByte( addr: Long ): Int = {
		pos = addr
		getByte
	}
	
	def putByte( addr: Long, b: Int ) {
		pos = addr
		putByte( b )
	}
	
	def readByteChars( len: Int ) = {
		val buf = new StringBuilder
		
		for (_ <- 1 to len)
			buf += getByte.asInstanceOf[Char]
			
		buf.toString
	}

	def getByteString = {
		if (remaining >= 1) {
			val len = getUnsignedByte
			
			if (remaining >= len)
				Some( readByteChars(len) )
			else
				None
		}
		else
			None
	}
	
	def putByteString( s: String ) {
		putByte( s.length.asInstanceOf[Byte] )
		writeByteChars( s )
	}
	
	def getString( len: Int, cs: Charset = charset ) = new String( getBytes(len), cs )

	def getType: Int =
		getUnsignedByte match {
			case POINTER => getUnsignedByte( getBig )
			case t => t
		}

	def getType( addr: Long ): Int = {
		pos = addr
		getType
	}
	
	def getTimestamp = Instant.ofEpochMilli( getLong )
	
	def putTimestamp( t: Instant ) = putLong( t.toEpochMilli )
	
	def getDatetime = OffsetDateTime.of( getInt, getByte, getByte, getByte, getByte, getByte, getInt, ZoneOffset.ofTotalSeconds(getSmall) )
	
	def putDatetime( datetime: OffsetDateTime ) = {
		putInt( datetime.getYear )
		putByte( datetime.getMonthValue )
		putByte( datetime.getDayOfMonth )
		putByte( datetime.getHour )
		putByte( datetime.getMinute )
		putByte( datetime.getSecond )
		putInt( datetime.getNano )
		putSmall( datetime.getOffset.getTotalSeconds )
	}
	
	def getUUID = new UUID( getLong, getLong )
	
	def putUUID( id: UUID ) {
		putLong( id.getMostSignificantBits )
		putLong( id.getLeastSignificantBits )
	}
	
	def getBoolean =
		getByte match {
			case TRUE => true
			case FALSE => false
			case _ => sys.error( "invalid boolean value" )
		}
	
	def putBoolean( a: Boolean ) = putByte( if (a) TRUE else FALSE )
	
	object Type1 {
		def unapply( t: Int ): Option[(Int, Int)] = {
			Some( t&0xF0, t&0x0F )
		}
	}
	
	object Type2 {
		def unapply( t: Int ): Option[(Int, Int, Int)] = {
			Some( t&0xF0, t&0x04, t&0x03 )
		}
	}

	def getValue: Any = {
		val cur = pos
		val res =
			getType match {
				case NULL => null
				case NSTRING => ""
				case FALSE => false
				case TRUE => true
				case BYTE => getByte
				case SHORT => getShort
				case INT => getInt
				case LONG => getLong
				case TIMESTAMP => getTimestamp
				case DATETIME => getDatetime
				case UUID => getUUID
				case DOUBLE => getDouble
				case Type1( SSTRING, len ) => getString( len + 1 )
				case Type2( STRING, encoding, width ) =>
					val len =
						width match {
							case UBYTE_LENGTH => getUnsignedByte
							case USHORT_LENGTH => getUnsignedShort
							case INT_LENGTH => getInt
						}

					if (encoding == ENCODING_INCLUDED)
						getString( len, Charset.forName(getByteString.get) )
					else
						getString( len )
				case EMPTY => Map.empty
				case MEMBERS => getObject
				case NIL => Nil
				case ELEMENTS => getArray
			}
	
		pos = cur + vwidth
		res
	}
	
	def putValue( v: Any ) {
		v match {
			case null =>
				putByte( NULL )
				pad( cwidth )
			case "" =>
				putByte( NSTRING )
				pad( cwidth )
			case b: Boolean =>
				putBoolean( b )
				pad( cwidth )
			case a: Int if a.isValidByte =>
				putByte( BYTE )
				putByte( a )
				pad( cwidth - 1 )
			case a: Int if a.isValidShort =>
				val (io, p) = need( 2 )
				
				io.putByte( SHORT )
				io.putShort( a )
				pad( p )
			case a: Int =>
				val (io, p) = need( 4 )
				
				io.putByte( INT )
				io.putInt( a )
				pad( p )
			case a: Long if a.isValidByte =>
				putByte( BYTE )
				putByte( a.asInstanceOf[Int] )
				pad( cwidth - 1 )
			case a: Long if a.isValidShort =>
				val (io, p) = need( 2 )
				
				io.putByte( SHORT )
				io.putShort( a.asInstanceOf[Int] )
				pad( p )
			case a: Long if a.isValidInt =>
				val (io, p) = need( 4 )
				
				io.putByte( INT )
				io.putInt( a .asInstanceOf[Int] )
				pad( p )
			case a: Long =>
				val (io, p) = need( 8 )
				
				io.putByte( LONG )
				io.putLong( a )
				pad( p )
			case a: Instant =>
				val (io, p) = need( 8 )
				
				io.putByte( TIMESTAMP )
				io.putTimestamp( a )
				pad( p )
			case a: OffsetDateTime =>
				val (io, p) = need( DATETIME_WIDTH )
				
				io.putByte( DATETIME )
				io.putDatetime( a )
				pad( p )
			case a: UUID =>
				val (io, p) = need( 16 )
				
				io.putByte( UUID )
				io.putUUID( a )
				pad( p )
			case a: Double =>
				val (io, p) = need( 8 )
				
				io.putByte( DOUBLE )
				io.putDouble( a )
				pad( p )
			case a: String =>
				val s = encode( a )
				val (io, p) = need(
					s.length match {
						case l if l <= cwidth => l
						case l if l < 256 => l + 1
						case l if l < 65536 => l + 2
						case l => l + 4
					} )
				
				if (s.length > cwidth) {
					s.length match {
						case l if l < 256 =>
							io.putByte( STRING|UBYTE_LENGTH )
							io.putByte( l )
						case l if l < 65536 => 
							io.putByte( STRING|USHORT_LENGTH )
							io.putShort( l )
						case l =>
							io.putByte( STRING|INT_LENGTH )
							io.putInt( l )
					}
					
					io.putBytes( s )
				} else {
					io.putByte( SSTRING|(s.length - 1) )
					io.putBytes( s )
				}
				
				pad( p )
			case a: collection.Map[_, _] if a isEmpty =>
				putByte( EMPTY )
				pad( cwidth )
			case a: collection.Map[_, _] =>
				putByte( POINTER )

				val io = allocPad
				
				io.putByte( MEMBERS )
				io.putObject( a )
			case a: collection.TraversableOnce[_] if a isEmpty =>
				putByte( NIL )
				pad( cwidth )
			case a: collection.TraversableOnce[_] =>
				putByte( POINTER )

				val io = allocPad
				
				io.putByte( ELEMENTS )
				io.putArray( a )
			case a => sys.error( "unknown type: " + a )
		}
	}
	
	def getValue( addr: Long ): Any = {
		pos = addr
		getValue
	}
	
	def putValue( addr: Long, v: Any ) {
		pos = addr
		putValue( v )
	}
	
	def getObject = {
		var map = Map.empty[Any, Any]
		
		def chunk {
			val cont = getBig
			val len = getBig
			val start = pos

			while (pos - start < len) {
				if (getUnsignedByte == USED)
					map += getValue -> getValue
				else {
					skipValue
					skipValue
				}
			}
			
			if (cont != NUL) {
				pos = cont
				chunk
			}
		}

		skipBig	// skip last chunk pointer
		chunk
		map
	}
	
	def putObject( m: collection.Map[_, _] ) {
		padBig//putBig( pos + bwidth )	// last chunk pointer
		putObjectChunk( m )
	}
	
	def putObject( addr: Long, m: collection.Map[_, _] ) {
		pos = addr
		putObject( m )
	}
	
	def putObjectChunk( m: collection.Map[_, _] ) {
		padBig	// continuation pointer
		
		val start = pos
	
		padBig	// size of object in bytes
		
		for (p <- m)
			putPair( p )
	
		putBig( start, pos - start - bwidth )
	}
	
	def putPair( kv: (Any, Any) ) {
		putByte( USED )
		putValue( kv._1 )
		putValue( kv._2 )
	}
	
	def putPair( addr: Long, kv: (Any, Any) ) {
		pos = addr
		putPair( kv )
	}
	
	def putElement( v: Any ) {
		putByte( USED )
		putValue( v )
	}
	
	def getArray = {
		val buf = new ListBuffer[Any]
		
		def chunk {
			val cont = getBig
			val len = getBig
			val start = pos

			while (pos - start < len) {
				if (getUnsignedByte == USED)
					buf += getValue
				else
					skipValue
			}
			
			if (cont > 0) {
				pos = cont
				chunk
			}
		}

		skipBig						// skip count
		
		getBig match {				// set pos to first chunk
			case NUL => skipBig
			case f => pos = f
		}
		
		chunk
		buf.toList
	}
	
	def putArray( s: collection.TraversableOnce[Any] ) {
		val cur = pos
		
		padBig	// length
		padBig	// first chunk pointer
		padBig	// last chunk pointer
		putArrayChunk( s, this, cur )
	}
	
	def putArrayChunk( s: collection.TraversableOnce[Any], lengthio: IO, lengthptr: Long, contptr: Long = NUL ) {
		putBig( contptr )	// continuation pointer
		
		val start = pos
		var count = 0L
		
		padBig	// size of object in bytes
		
		for (e <- s) {
			putByte( USED )
			putValue( e )
			count += 1
		}
	
		putBig( start, pos - start - bwidth )
		lengthio.addBig( lengthptr, count )
	}

	//
	// Iterators
	//
	
	def objectIterator( addr: Long ): Iterator[Long] =
		getType( addr ) match {
			case EMPTY => Iterator.empty
			case MEMBERS =>
				val header = pos
				
				new AbstractIterator[Long] {
					var cont: Long = _
					var chunksize: Long = _
					var cur: Long = _
					var scan = false
					var done = false
					
					chunk( header + bwidth )
					
					private def chunk( p: Long ) {
						cont = getBig( p )
						chunksize = getBig
						cur = pos
					}
					
					def hasNext = {
						def advance = {
							chunksize -= pwidth
							
							if (chunksize == 0)
								if (cont == NUL) {
									done = true
									false
								} else {
									chunk( cont )
									true
								}
							else {
								cur += pwidth
								true
							}
						}

						def nextused: Boolean =
							if (advance)
								if (getUnsignedByte( cur ) == USED)
									true
								else
									nextused
							else
								false
						
						if (done)
							false
						else if (scan)
							if (nextused) {
								scan = false
								true
							} else
								false
						else
							true
					}
					
					def next =
						if (hasNext) {
							scan = true
							cur
						} else
							throw new NoSuchElementException( "next on empty objectIterator" )
				}
			case _ => sys.error( "can only use 'objectIterator' for an object" )
		}
		
	def arrayIterator( addr: Long ) =
		getType( addr ) match {
			case NIL => Iterator.empty
			case ELEMENTS =>
				val header = pos
				
				skipBig
				
				val first = getBig match {
					case NUL => header + 3*bwidth
					case p => p
				}
				
				new AbstractIterator[Long] {
					var cont: Long = _
					var chunksize: Long = _
					var cur: Long = _
					var done = false

					chunk( first )
					nextused

					private def chunk( p: Long ) {
						cont = getBig( p )
						chunksize = getBig
						cur = pos
					}

					private def nextused {
						if (chunksize == 0)
							if (cont == NUL)
								done = true
							else {
								chunk( cont )
								nextused
							}
						else {
							chunksize -= ewidth

							if (getUnsignedByte( cur ) == UNUSED) {
								cur += ewidth
								nextused
							}
						}
					}

					def hasNext = !done

					def next =
						if (done)
							throw new NoSuchElementException( "next on empty arrayIterator" )
						else {
							val res = cur

							cur += ewidth
							nextused
							res
						}
				}
			case _ => sys.error( "can only use 'arrayIterator' for an array" )
		}

	//
	// utility methods
	//
	
	def remaining: Long = size - pos
	
	def atEnd = pos == size - 1
	
	def dump {
		val cur = pos
		val width = 16
		
		pos = 0
		
		def printByte( b: Int ) = print( "%02x ".format(b&0xFF).toUpperCase )
		
		def printChar( c: Int ) = print( if (' ' <= c && c <= '~') c.asInstanceOf[Char] else '.' )
		
		for (line <- 0L until size by width) {
			printf( s"%10x  ", line )
			
			val mark = pos
			
			for (i <- line until ((line + width) min size)) {
				if (i%16 == 8)
					print( ' ' )
					
				printByte( getByte )
			}
			
			val bytes = (pos - mark).asInstanceOf[Int]
			
			print( " "*((width - bytes)*3 + 1 + (if (bytes < 9) 1 else 0)) )
			
			pos = mark
			
			for (_ <- line until ((line + width) min size))
				printChar( getByte )
				
			println
		}
		
		pos = cur
		println
	}

	def skip( len: Long ) = pos += len
	
	def skipByte = pos += 1
	
	def skipByte( addr: Long ) {
		pos = addr
		skipByte
	}
	
	def skipType( addr: Long ) {
		if (getUnsignedByte( addr ) == POINTER)
			skipByte( getBig )
	}
	
	def skipBig = skip( bwidth )
	
	def skipInt = skip( 4 )
	
	def skipLong = skip( 8 )
	
	def skipDouble = skip( 8 )
	
	def skipValue = skip( vwidth )
	
	def pad( n: Long ) =
		for (_ <- 1L to n)
			putByte( 0 )
			
	def padBig = pad( bwidth )
	
	def padCell = pad( cwidth )
	
	def encode( s: String ) = s.getBytes( charset )
	
	def inert( action: => Unit ) {
		val cur = pos
		
		action
		
		pos = cur
	}
	
	def need( width: Int ) = {
		if (width > cwidth) {
			putByte( POINTER )
			(alloc, cwidth)
		} else
			(this, cwidth - width)
	}
	
	def todo = sys.error( "not implemented" )
	
	//
	// allocation
	//
	
	private [bittydb] val allocs = new ListBuffer[AllocIO]
	private [bittydb] var appendbase: Long = _
	
	private [bittydb] def bitCeiling( n: Long ) = {
		val highest = java.lang.Long.highestOneBit( n )
		
		if ((highest^n) == 0)
			highest
		else
			highest << 1
	}
	
	def remove( addr: Long ) {
		if (getUnsignedByte( addr ) == POINTER) {
			val p = getBig( addr + 1 )
			
			getUnsignedByte( p ) match {
				case MEMBERS =>
					for (m <- objectIterator( p )) {
						remove( m + 1 )
						remove( m + 1 + vwidth )
					}
				case ELEMENTS =>
					for (e <- arrayIterator( p ))
						remove( e + 1 )
				case _ =>
			}
			
			dealloc( p )
			putByte( addr, NULL )
		}
	}
	
	def bucketPtr( bucketIndex: Int ) = bucketIndex*bwidth + bucketsPtr
	
	def dealloc( p: Long ) {
		val ind = getByte( p - 1 )
		
		putBig( p, buckets(ind) )
		buckets(ind) = p
		putBig( bucketPtr(ind), p )
	}
	
	def alloc = {
		val res = new AllocIO( this )
		
		allocs += res
		res.backpatch( this, pos )
		res
	}
	
	def allocPad = {
		val res = alloc
		
		padCell
		res
	}
	
	private [bittydb] def placeAllocs( io: IO ) {
		for (a <- allocs) {
			io.buckets( a.bucket ) match {
				case NUL =>
					a.base = io.appendbase + 1
					io.appendbase += 1 + a.size
				case p =>
					val ptr = io.getBig( p )
				
					io.buckets( a.bucket ) = ptr
					io.putBig( bucketPtr(a.bucket), ptr )
					a.base = p
			}
			
			a.placeAllocs( io )
		}
	}
	
	private [bittydb] def writeAllocBackpatches {
		for (a <- allocs) {
			a.writeBackpatches
			a.writeAllocBackpatches
		}
	}
	
	private [bittydb] def writeAllocs( dest: IO ) {
		for (a <- allocs) {
			dest.pos = a.base - 1
			dest.putByte( a.bucket )
			dest.writeBuffer( a.asInstanceOf[MemIO] )
			a.writeAllocs( dest )
		}
	}
	
	def finish {
		if (allocs.nonEmpty)
		{
			append
			appendbase = pos
			placeAllocs( this )
			writeAllocBackpatches
			writeAllocs( this )			
			allocs.clear
		}
		
		force
	}
}