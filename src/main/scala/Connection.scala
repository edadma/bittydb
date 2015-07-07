package ca.hyperreal.bittydb

import java.io.File
import java.nio.charset.Charset
import java.nio.charset.StandardCharsets.UTF_8


object Connection
{
	def disk( file: String, charset: Charset = UTF_8 ): Connection = disk( new File(file), charset )
	
	def disk( f: File, charset: Charset ) = new Connection( new DiskIO(f), charset )
	
	def mem( charset: Charset = UTF_8 ) = new Connection( new MemIO, charset )
}

class Connection( private [bittydb] val io: IO, charset: Charset ) extends IOConstants
{
	private [bittydb] var version: String = _
	private [bittydb] var freeListPtr: Long = _
	private [bittydb] var freeList: Long = _
	private [bittydb] var root: Pointer = _
	
	if (io.size == 0) {
		version = VERSION
		io putByteString s"BittyDB $version"
		io.charset = charset
		io putByteString charset.name
		freeListPtr = io.pos
		freeList = 0
		io putBig freeList
		root = new Pointer( io.pos )
		io putByte OBJECT
		io putObject Map.empty
		io.force
	}
	else
		io.getByteString match {
			case Some( s ) if s startsWith "BittyDB " =>
				log( s )
				
				version = s substring 8
				
				if (version > VERSION)
					sys.error( "attempting to read database of newer format version" )
					
				io.getByteString match {
					case Some( cs ) =>
						io.charset = Charset.forName( cs )
						freeListPtr = io.pos
						freeList = io.getBig
						root = new Pointer( io.pos )
					case _ => invalid
				}
			case _ => invalid
		}
	
	def invalid = sys.error( "invalid database" )
	
	override def toString = "connection to " + io
	
	class Pointer( addr: Long ) {
		def get = io.getValue( addr )
		
		def put( v: Any ) {
			io.putValue( addr, v )
		}
		
// 		def find( key: Any ): (Boolean, Option[Long]) =
// 			io.getByte( addr ) match {
// 				case EMPTY =>
// 					sys.error( "empty object" )//(false, None)
// 				case OBJECT =>
// 					val cont = io.getBig
// 					val len = io.getBig
// 					val start = io.pos
// 					var where: Option[Long] = None
// 					
// 					while (io.pos - start < len) {
// 						val addr = pos
// 						
// 						if (io.getByte == USED)
// 							if (io.getValue == key)
// 								return (true, Some( addr ))
// 							else
// 								io.skipValue
// 					}
// 				case _ =>
// 					sys.error( "can only use 'find' for an object" )
// 			}
		
		def set( kv: (Any, Any) ) {
			io.getByte( addr ) match {
				case EMPTY =>
					io.putValue( addr, Map(kv) )
				case OBJECT =>
					io.putValue( addr, io.getValue(addr).asInstanceOf[Map[Any, Any]] + kv )
				case _ =>
					sys.error( "can only use 'set' for an object" )
			}
		}
	}
}