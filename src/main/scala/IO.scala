package xyz.hyperreal.bittydb

import java.io.InputStream
import java.nio.charset.Charset
import java.nio.charset.StandardCharsets.UTF_8
import java.time.{Instant, OffsetDateTime, ZoneOffset}
import java.util.{NoSuchElementException, UUID}

import collection.AbstractIterator
import collection.mutable.{ArrayStack, ListBuffer}


object IO {
	private [bittydb] val pwidth_default = 5
	private [bittydb] val cwidth_default = 8
}

abstract class IO extends IOConstants {
	private [bittydb] var charset = UTF_8
	private [bittydb] var pwidth = IO.pwidth_default					// pointer width
	private [bittydb] var cwidth = IO.cwidth_default					// cell width
	private [bittydb] var bucketsPtr: Long = _
	private [bittydb] var buckets: Array[Long] = _
	private [bittydb] var uuidOption: Boolean = true

	private [bittydb] lazy val maxsize = 1L << pwidth*8
	private [bittydb] lazy val vwidth = 1 + cwidth						// value width
	private [bittydb] lazy val twidth = 1 + 2*vwidth 					// pair width
//	private [bittydb] lazy val ewidth = 1 + vwidth						// element width
	private [bittydb] lazy val minblocksize = bitCeiling( vwidth + 1 ).toInt		// smallest allocation block needed
	private [bittydb] lazy val sizeShift = Integer.numberOfTrailingZeros( minblocksize )
	private [bittydb] lazy val bucketLen = pwidth*8 - sizeShift

//	println( pwidth, lowestSize, sizeShift, bucketLen )

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

	def putBytes( a: Array[Byte], offset: Int, length: Int )

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

	def peekUnsignedByte: Int = {
		val b = getUnsignedByte

		pos -= 1
		b
	}

	def peekUnsignedByte( addr: Long ): Int = {
		pos = addr
		peekUnsignedByte
	}

	def getSmall: Int = (getByte << 16) | (getUnsignedByte << 8) | getUnsignedByte

	def putSmall( a: Int ): Unit = {
		putByte( a >> 16 )
		putByte( a >> 8 )
		putByte( a )
	}

	def getBig: Long = {
		var res = 0L

		for (_ <- 1 to pwidth) {
			res <<= 8
			res |= getUnsignedByte
		}

		res
	}

	def putBig( l: Long ) {
		if (l > maxsize)
			sys.error( "pointer value too large" )

		for (shift <- (pwidth - 1)*8 to 0 by -8)
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

	def addBig( a: Long ) = {
		val cur = pos
		val sum = getBig + a

		pos = cur
		putBig( sum )
		sum
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
		} else
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

	private def bool2int( a: Boolean ) = if (a) TRUE else FALSE

	def putBoolean( a: Boolean ) = putByte( bool2int(a) )

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
				case EMPTY_ARRAY => Array()
				case ARRAY_ELEMS => getArray
				case ARRAY_MEMS => getArrayObject
				case NIL => Nil
				case LIST_ELEMS => getList
				case LIST_MEMS => getListObject
				case BLOB => getBlob
			}

		pos = cur + vwidth
		res
	}

	def putAlloc( t: Int ) = {
		putByte( POINTER )

		val io = allocPad

		io.putByte( t )
		io
	}

	def putSimple( t: Int ) = {
		putByte( t )
		pad( cwidth )
	}

	def putValue( v: Any ) {
		v match {
			case null => putSimple( NULL )
			case "" => putSimple( NSTRING )
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
			case a: collection.Map[_, _] if a isEmpty => putSimple( EMPTY )
			case a: collection.Map[_, _] => putAlloc( LIST_MEMS ).putListObject( a )//putAlloc( ARRAY_MEMS ).putArrayObject( a )
			case a: collection.IndexedSeq[_] if a isEmpty => putSimple( EMPTY_ARRAY )
			case a: collection.IndexedSeq[_] => putAlloc( ARRAY_ELEMS ).putArray( a )
			case a: collection.TraversableOnce[_] if a isEmpty => putSimple( NIL )
			case a: collection.TraversableOnce[_] => putAlloc( LIST_ELEMS ).putList( a )
			case a: Blob => putAlloc( BLOB ).putBlob( a )
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

	def getBlob = new IOBlob( this, pos + pwidth, getBig )

	def putBlob( a: Blob ): Unit = {
		putBig( a.length )

		val s = a.stream
		val buf = new Array[Byte]( 1000 )
		var len = 0

		while ({len = s.read( buf ); len != -1})
			putBytes( buf, 0, len )

		s.close
	}

	def putArrayObject( m: collection.Map[_, _] ): Unit = {
		putBig( m.size*2 )

		for ((k, v) <- m) {
			putValue( k )
			putValue( v )
		}
	}

	def getArrayObject = {
		val pairs =
			for (_ <- 1L to getBig/2)
				yield {
					getValue -> getValue
				}

		Map( pairs: _* )
	}

	def getListObject = listElemsIterator grouped 2 map {case Seq((_, k), (_, v)) => (getValue( k ), getValue( v ))} toList

	def putListObject( m: collection.Map[_, _] ): Unit = putList( m.iterator.flatMap(p => p.productIterator) )

	def putListObject( addr: Long, m: collection.Map[_, _] ) {
		pos = addr
		putListObject( m )
	}

	def getArray = arrayElemsIterator map getValue toList

	def putArray( a: collection.IndexedSeq[Any] ): Unit = {
		putBig( a.length )
		a foreach putValue
	}

	def getList = listElemsIterator map {case (_, e) => getValue( e )} toList

	def putList( s: collection.TraversableOnce[Any] ) {
		padBig	// last chunk pointer
		padBig	// chunks with freed elements list pointer

		val cur = pos

		padBig	// length
		putListChunk( s, this, cur )
	}

	def putListChunk( s: collection.TraversableOnce[Any], lengthio: IO, lengthptr: Long, contptr: Long = NUL ) {
		putBig( contptr )	// continuation pointer
		padBig	// next chunk with freed elements pointer
		padBig	// free pointer

		val lenptr = pos

		padBig	// length of chunk in bytes

		val countptr = pos

		padBig	// count of elements

		val elemsptr = pos
		var count = 0L

		for (e <- s) {
			putValue( e )
			count += 1
		}

		putBig( lenptr, pos - elemsptr )
		putBig( countptr, count )
		lengthio.addBig( lengthptr, count )
	}

	def byteInputStream( addr: Long, length: Long ) = {
		require( size >= 0, "size must be non-negative" )

		new InputStream {
			var cur = addr

			def read = {
				if (cur < addr + length) {
					val res = getByte( cur )

					cur += 1
					res
				} else
					-1
			}
		}
	}

	//
	// Iterators
	//

	def listObjectIterator( addr: Long ): Iterator[(Long, Long)] =
		getType( addr ) match {
			case EMPTY => Iterator.empty
			case LIST_MEMS => listElemsIterator grouped 2 map {case Seq((_, k), (_, v)) => (k, v)}
			case _ => sys.error( "can only use 'listObjectIterator' for a list object" )
		}


	private def arrayElemsIterator =
		new AbstractIterator[Long] {
			var count = getBig
			var cur = pos

			def hasNext = count > 0

			def next =
				if (hasNext) {
					val res = cur

					cur += cwidth
					count -= 1
					res
				} else
					throw new NoSuchElementException( "next on empty arrayElemsIterator" )
		}

	def arrayIterator( addr: Long ) =
		getType( addr ) match {
			case NIL => Iterator.empty
			case ARRAY_ELEMS => arrayElemsIterator
			case _ => sys.error( "can only use 'arrayIterator' for an array" )
		}

	def arrayObjectIterator( addr: Long ) =
		getType( addr ) match {
			case NIL => Iterator.empty
			case ARRAY_MEMS =>
				new Iterator[Long] {
					val it = arrayElemsIterator

					def hasNext = it.hasNext

					def next = {
						val res = it.next

						it.next // discard every second value
						res
					}
				}
			case _ => sys.error( "can only use 'arrayObjectIterator' for an object (stored as an array)" )
		}

	private def listElemsIterator = {
		val header = pos
		val first = header + 3*pwidth
//			getBig match {
//				case NUL => header + 4*pwidth
//				case p => p
//			}

		new AbstractIterator[(Long, Long)] {
			var cont: Long = _
			var chunksize: Long = _
			var chunkptr: Long = _
			var cur: Long = _
			var done = false

			chunk( first )
			nextused

			private def chunk( p: Long ) {
				chunkptr = p
				cont = getBig( p )
				skipBig		// next freed pointer
				skipBig		// free pointer
				chunksize = getBig
				skipBig		// skip count
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
					chunksize -= vwidth

					if (peekUnsignedByte( cur ) == DELETED) {
						cur += vwidth
						nextused
					}
				}
			}

			def hasNext = !done

			def next =
				if (done)
					throw new NoSuchElementException( "next on empty listElemsIterator" )
				else {
					val res = (chunkptr, cur)

					cur += vwidth
					nextused
					res
				}
		}
	}

	def listIterator( addr: Long ) =
		getType( addr ) match {
			case NIL => Iterator.empty
			case LIST_ELEMS => listElemsIterator
			case _ => sys.error( "can only use 'listIterator' for a list" )
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

	def check: Unit = {
		val stack = new ArrayStack[String]
//		val reclaimed = new ArrayBuffer[(Long, Long)]	//todo: use in checking reclaimed storage

		def problem( msg: String, adjust: Int = 0 ) {
			println( f"${pos - adjust}%16x: $msg" )

			for (item <- stack)
				println( item )

//			dump
			sys.error( "check failed" )
		}

		def push( item: String, adjust: Int = 0 ) : Unit =
			stack push f"${pos - adjust}%16x: $item"

		def pop = stack.pop

		def checkif( c: Boolean, msg: String, adjust: Int = 0 ) =
			if (!c)
				problem( msg, adjust )

		def checkbytes( n: Int ) = checkif( remaining >= n, f"${n - remaining}%x past end" )

		def checkubyte = {
			checkbytes( 1 )
			getUnsignedByte
		}

		def checkushort = {
			checkbytes( 2 )
			getUnsignedShort
		}

		def checkint = {
			checkbytes( 4 )
			getInt
		}

		def checkbig = {
			checkbytes( pwidth )

			val p = getBig

			checkif( 0 <= p && p < size - 1, f"pointer out of range: $p%x", pwidth )
			// todo: check if pointer points to reclaimed space
			p
		}

		def checkbytestring( s: String ) = {
			val l = checkubyte

			checkif( l > 0, s"byte string size should be positive: $l" )
			checkbytes( l )

			val chars = readByteChars( l )

			if (s ne null)
				checkif( chars == s, "incorrect byte string", l )

			(chars, l + 1)
		}

		def checkbyterange( from: Int, to: Int ) = {
			val b = checkubyte

			checkif( from <= b && b <= to, s"byte out of range: $from to $to" )
			b
		}

		def checkpos( p: Long, adjust: Int = 0 ): Unit = {
			checkif( 0 < p && p < size, f"invalid file position: $p%x", adjust )
			pos = p
		}

		def checkvalue: Unit = {
			val cur = pos

			checkbytes( vwidth )
			// todo: check if we've wondered into reclaimed space

			checkubyte match {
				case POINTER =>
					checkpos( checkbig, pwidth )
					checkdata( checkubyte )
				case t => checkdata( t )
			}

			pos = cur + vwidth
		}

		def checkdata( t: Int ): Unit = {
			// todo: check if allocation block is the correct size
			t match {
				case NULL|NSTRING|FALSE|TRUE|EMPTY|EMPTY_ARRAY|NIL|BYTE|SHORT|INT|LONG =>
				case BIGINT => sys.error( "BIGINT" )
				case DOUBLE => sys.error( "DOUBLE" )
				case Type1( SSTRING, l ) =>
					push( "small string", 1 )
					checkif( 0 <= l && l <= 0xF, s"small string length out of range: $l", 1 )
					skip( cwidth )
					pop
				case Type2( STRING, encoding, width ) =>
					push( "string" )
					val len =
						width match {
							case UBYTE_LENGTH => checkubyte
							case USHORT_LENGTH => checkushort
							case INT_LENGTH => checkint
						}

					if (encoding == ENCODING_INCLUDED) {
						val (cs, css) = checkbytestring( null )

						checkif( Charset.isSupported( cs ), s"charset not supported: $cs", css )
					}

					checkbytes( len )
					skip( len )
					pop
				case ARRAY_ELEMS|ARRAY_MEMS =>
					push( "array", 1 )

					for (_ <- 1L to checkbig)
						checkvalue

					pop
				case LIST_ELEMS|LIST_MEMS =>
					push( "list", 1 )

//					val first = checkbig
					val lastptr = pos
					val last =
						checkbig match {
							case NUL => pos + 2*pwidth
							case l => l
						}
					/*val freed =*/ checkbig	//todo: use freed check free list
					val countptr = pos
					val count = checkbig

					var elemcount = 0

					def chunk {
						val chunkheader = pos

						var chunkelemcount = 0

						push( "list chunk" )
						push( "next chunk pointer" )
						val cont = checkbig
						pop

						push( "next freed pointer" )
						/*val nfree =*/ checkbig	//todo: use nfree to check free list
						pop

						push( "free pointer" )
						/*val free =*/ checkbig	//todo: use free to check free list
						pop

						push( "chunk length" )
						val len = checkbig
						checkif( 0 < len && len%vwidth == 0, "must be positive and a multiple of vwidth", pwidth )
						pop

						push( "chunk count" )
						val countptr = pos
						val count = checkbig
						checkif( 0 <= count && count <= len/vwidth, "must be non-negative and less than or equal to length/vwidth", pwidth ) // todo: count should never be 0 if empty chunks are removed immediately
						pop

						val start = pos

						while (pos - start < len)
							if (peekUnsignedByte == DELETED) {
								checkbytes( vwidth )
								skipValue
							} else {
								checkvalue
								elemcount += 1
								chunkelemcount += 1
							}

						pos = countptr
						checkif( chunkelemcount == count, "incorrect chunk count" )

						if (cont > 0) {
							checkpos( cont )
							chunk
						} else {
							pos = lastptr
							checkif( chunkheader == last, "incorrect last chunk pointer" )
						}

						pop
					}

//					if (first != NUL)
//						checkpos( first, pwidth )

					chunk
					pos = countptr
					checkif( elemcount == count, s"count is incorrect: count is $count but read $elemcount elements" )
					pop
				case b => problem( f"unknown type byte: $b%02x", 1 )
			}
		}

		def checkbucket( block: Long, bucket: Int ) {
			if (block != NUL) {
				push( s"bucket $bucket", pwidth )
				checkpos( block - 1 )
				checkif( checkubyte == bucket, "incorrect bucket index byte", 1 )
				checkbucket( checkbig, bucket )
				pop
			}
		}

		pos = 0
		push( "file header" )
		push( "file type" )
		checkbytestring( "BittyDB" )
		pop

		push( "format version" )
		checkbytestring( null )
		pop

		push( "charset" )
		checkbytestring( null )
		pop

		push( "pointer width" )
		checkif( checkbyterange(1, 8) == pwidth, "changed", 1 )
		pop

		push( "cell width" )
		checkif( checkbyterange(1, 16) == cwidth, "changed", 1 )
		pop

		push( "uuid" )
		checkif( checkbyterange(FALSE, TRUE) == bool2int(uuidOption), "changed", 1 )
		pop

		push( "buckets" )
		checkif( bucketLen == buckets.length, "lengths don't match" )

		for (i <- 0 until bucketLen) {
			checkif( checkbig == buckets(i), f"pointer mismatch - bucket array has ${buckets(i)}%x", pwidth )

			val bucket = pos

			checkbucket( buckets(i), i )
			pos = bucket
		}

		pop
		pop

		push( "root" )
		checkvalue
		pop
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

	def skipBig = skip( pwidth )

	def skipInt = skip( 4 )

	def skipLong = skip( 8 )

	def skipDouble = skip( 8 )

	def skipValue = skip( vwidth )

	def skipCell = skip( cwidth )

	def pad( n: Long ) =
		for (_ <- 1L to n)
			putByte( 0 )

	def padBig = pad( pwidth )

	def padCell = pad( cwidth )

	def encode( s: String ) = s.getBytes( charset )

	def inert( action: => Unit ) {
		val cur = pos

		action
		pos = cur
	}

	def need( width: Int ) =
		if (width > cwidth) {
			putByte( POINTER )
			(alloc, cwidth)
		} else
			(this, cwidth - width)

	def todo = sys.error( "not implemented" )

	//
	// allocation
	//

	private [bittydb] val allocs = new ListBuffer[AllocIO]
	private [bittydb] var appendbase: Long = _

	def remove( addr: Long ) {
		if (getUnsignedByte( addr ) == POINTER) {
			val p = getBig

			getUnsignedByte( p ) match {
				case LIST_ELEMS =>
					for ((_, e) <- listIterator( p ))
						remove( e )
				case LIST_MEMS =>
					for ((k, v) <- listObjectIterator( p )) {
						remove( k )
						remove( v )
					}
				case ARRAY_ELEMS =>
					arrayIterator( p ) foreach remove
				case ARRAY_MEMS =>
					for (m <- arrayObjectIterator( p )) {
						remove( m )
						remove( m + vwidth )
					}
				case _ =>
			}
			
			dealloc( p )
		}
	}
	
	def bucketPtr( bucketIndex: Int ) = bucketIndex*pwidth + bucketsPtr
	
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
					io.appendbase += a.allocSize
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
			dest.writeBuffer( a )
			dest.pad( a.allocSize - a.size - 1 )
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