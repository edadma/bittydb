package ca.hyperreal.bittydb

import java.nio.charset.Charset
import java.nio.charset.StandardCharsets.UTF_8
import java.time.{Instant, OffsetDateTime, ZoneOffset}
import java.util.UUID

import collection.mutable.{ListBuffer, HashMap}


abstract class IO extends IOConstants
{
	private [bittydb] var charset: Charset = UTF_8
	private [bittydb] var bwidth = 5					// big (i.e. pointers, sizes) width (2 minimum)
	private [bittydb] var kwidth = 8					// key width
	private [bittydb] var vwidth = 8					// value width
	private [bittydb] var ewidth = 8					// element width
	
	//
	// abstract methods
	//
	
	def allocSize = allocs map (_.totalSize) sum
	
	def close
	
	def force
	
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
	
	def getBig: Long = {//(getByte.asInstanceOf[Long]&0xFF)<<32 | getInt.asInstanceOf[Long]&0xFFFFFFFFL
		var res = 0L
		
		for (_ <- 1 to bwidth) {
			res <<= 8
			res |= getUnsignedByte
		}
		
		res
	}
	
	def putBig( l: Long ) {
		putByte( (l>>32).asInstanceOf[Int] )
		putInt( l.asInstanceOf[Int] )
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
	
// 	def getLen: Int = {
// 		var len = 0
// 		
// 		def read {
// 			val b = getByte
// 			len = (len << 7) | (b&0x7F)
// 			
// 			if ((b&0x80) != 0)
// 				read
// 		}
// 
// 		read
// 		len
// 	}
// 	
// 	def putLen( l: Int ) {
// 		if (l == 0)
// 			putByte( 0 )
// 		else {
// 			var downshift = 28
// 			var compare = 0x10000000
// 			
// 			while (l < compare) {
// 				downshift -= 7
// 				compare >>= 7
// 			}
// 			
// 			while (downshift >= 0) {
// 				putByte( (if (downshift > 0) 0x80 else 0) | ((l >> downshift)&0x7F) )
// 				downshift -= 7
// 			}
// 		}
// 	}
	
	def getString( len: Int, cs: Charset = charset ) = new String( getBytes(len), cs )
	
// 	def putString( s: Array[Byte] ) {
// 		putBytes( s )
// 	}
// 	
// 	def putString( s: String ) {putString( encode(s) )}

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
	
	def getDatetime = OffsetDateTime.of( getInt, getByte, getByte, getByte, getByte, getByte, getInt, ZoneOffset.ofTotalSeconds(getInt) )
	
	def putDatetime( datetime: OffsetDateTime ) = {
		putInt( datetime.getYear )
		putByte( datetime.getMonthValue )
		putByte( datetime.getDayOfMonth )
		putByte( datetime.getHour )
		putByte( datetime.getMinute )
		putByte( datetime.getSecond )
		putInt( datetime.getNano )
		putInt( datetime.getOffset.getTotalSeconds )
	}
	
	def getUUID = new UUID( getLong, getLong )
	
	def putUUID( id: UUID ) {
		putLong( id.getMostSignificantBits )
		putLong( id.getLeastSignificantBits )
	}
	
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

					if (encoding == WITH_ENCODING)
						getString( len, Charset.forName(getByteString.get) )
					else
						getString( len )
				case EMPTY => Map.empty
				case MEMBERS => getObject
				case NIL => Nil
				case ELEMENTS => getArray
			}
	
		pos = cur + VWIDTH
		res
	}
	
	def putValue( v: Any ) {
		v match {
			case null =>
				putByte( NULL )
				pad( 8 )
			case false =>
				putByte( FALSE )
				pad( 8 )
			case true =>
				putByte( TRUE )
				pad( 8 )
			case a: Int if a.isValidByte =>
				putByte( BYTE )
				putByte( a )
				pad( 7 )
			case a: Int if a.isValidShort =>
				putByte( SHORT )
				putShort( a )
				pad( 6 )
			case a: Int =>
				putByte( INT )
				putInt( a )
				pad( 4 )
			case a: Long if a.isValidByte =>
				putByte( BYTE )
				putByte( a.asInstanceOf[Int] )
				pad( 7 )
			case a: Long if a.isValidShort =>
				putByte( SHORT )
				putShort( a.asInstanceOf[Int] )
				pad( 6 )
			case a: Long if a.isValidInt=>
				putByte( INT )
				putInt( a .asInstanceOf[Int])
				pad( 4 )
			case a: Long =>
				putByte( LONG )
				putLong( a )
			case a: Instant =>
				putByte( TIMESTAMP )
				putTimestamp( a )
			case a: OffsetDateTime =>
				putByte( POINTER )
	
				val io = allocPrimitive
				
				io.putDatetime( a )
			case a: UUID =>
				putByte( POINTER )
	
				val io = allocPrimitive
				
				io.putByte( UUID )
				io.putUUID( a )
			case a: Double =>
				putByte( DOUBLE )
				putDouble( a )
			case a: String =>
				val cur = pos
			
				putString( a )
				pad( VWIDTH - (pos - cur) )
			case a: collection.Map[_, _] if a isEmpty =>
				putByte( EMPTY )
				pad( 8 )
			case a: collection.Map[_, _] =>
				putByte( POINTER )
		
				val io = allocComposite
				
				io.putByte( MEMBERS )
				io.putObject( a )
			case a: collection.TraversableOnce[_] if a isEmpty =>
				putByte( NIL )
				pad( 8 )
			case a: collection.TraversableOnce[_] =>
				putByte( POINTER )
		
				val io = allocComposite
				
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
	
	def putString( a: String ) {
		val s = encode( a )
		val io =
			if (s.length > VWIDTH - 1) {
				putByte( POINTER )
				allocPrimitive
			} else
				this
		
		if (s.isEmpty || s.length > SSTRING_MAX) {
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
	}
	
	def getObject = {
		var map = Map.empty[Any, Any]
		
		def chunk {
			val cont = getBig
			val len = getBig
			val start = pos

			while (pos - start < len) {
				if (getByte == USED)
					map += getValue -> getValue
				else {
					skipValue
					skipValue
				}
			}
			
			if (cont > 0) {
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
				if (getByte == USED)
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
			
			for (i <- line until ((line + width) min size))
				printChar( getByte.asInstanceOf[Int] )
				
			println
		}
		
		pos = cur
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
	
//	def skipString = skip( getLen )
	
	def skipValue = skip( 9 )
	
	def pad( n: Long ) =
		for (_ <- 1L to n)
			putByte( 0 )
			
	def padBig = pad( bwidth )
	
	def padValue = pad( 8 )
	
	def encode( s: String ) = s.getBytes( charset )
	
	def inert( action: => Unit ) {
		val cur = pos
		
		action
		
		pos = cur
	}
	
	//
	// allocation
	//
		
	private [bittydb] val allocs = new ListBuffer[AllocIO]
	private [bittydb] var primitive: AllocIO = null
	
	def allocPrimitive = {
		if (primitive eq null) {
			primitive = new AllocIO( charset )
			allocs += primitive
		}
		else
			primitive
		
		primitive.backpatch( this, pos )
		padValue
		primitive
	}
	
	def allocComposite = {
		val res = new AllocIO( charset )
		
		allocs += res
		res.backpatch( this, pos )
		padValue
		res
	}
	
	def writeAllocs( dest: IO, base: Long ) {
		var offset = base
		
		inert {
			for (a <- allocs) {
				a.writeBackpatches( offset )
				offset += a.size
			}
		}
		
		if (dest ne this)
			dest.writeBuffer( this.asInstanceOf[MemIO] )
			
		for (a <- allocs) {
			a.writeAllocs( dest, offset )
			offset += a.size
		}
	}
	
	def finish {
		if (!allocs.isEmpty)
		{
		val len = allocSize
		
			// allocate space
			append
			writeAllocs( this, pos )
			allocs.clear
			primitive = null
		}
		
		force
	}
}