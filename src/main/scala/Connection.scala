package ca.hyperreal.bittydb

import java.io.File
import java.nio.charset.Charset
import java.nio.charset.StandardCharsets.UTF_8

import util.Either


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
		
		private [bittydb] def lookup( key: Any ): Either[Option[Long], Long] = {
			val cont = io.getBig
			val len = io.getBig
			val start = io.pos
			var where: Option[Long] = None
			
			while (io.pos - start < len) {
				val addr = io.pos
				
				if (io.getByte == USED)
					if (io.getValue == key)
						return Right( addr )
					else
						io.skipValue
				else {
					if (where == None)
						where = Some( io.pos - 1 )
						
					io.skipValue
					io.skipValue
				}
			}
			
			Left( where )
		}
		
		def remove( key: Any ) =
			io.getType( addr ) match {
				case EMPTY => false
				case OBJECT =>
					lookup( key ) match {
						case Left( _ ) => false
						case Right( at ) =>
							io.putByte( at, UNUSED )
							true
					}
				case _ => sys.error( "can only use 'remove' for an object" )
			}
		
		def find( key: Any ): Option[Pointer] =
			io.getType( addr ) match {
				case EMPTY => None
				case OBJECT => None
				case _ => sys.error( "can only use 'find' for an object" )
			}
		
		private [bittydb] def atEnd =
			io.getType( addr ) match {
				case OBJECT =>
					io.skipBig
					io.pos + BWIDTH + io.getBig == io.size
				case STRING => sys.error( "not yet" )
				case BIGINT => sys.error( "not yet" )
				case DECIMAL => sys.error( "not yet" )
				case ARRAY => sys.error( "not yet" )
			}
			
		def set( kv: (Any, Any) ) {
			io.getType( addr ) match {
				case EMPTY => io.putValue( addr, Map(kv) )
				case OBJECT =>
					lookup( kv._1 ) match {
						case Left( None ) =>
							if (atEnd) {
								io.skipByte( addr )
								io.skipBig
								io.addBig( PWIDTH )
								io.append
								
								val a = new AllocIO( io )
								
								a.putPair( kv )
								a.done( true )
							}
							else {
								io.skipByte( addr )
								io.putBig( io.size )
								io.append
								io.putObject( Map(kv) )
							}
						case Left( Some(insertion) ) =>
							io.putPair( insertion, kv )
						case Right( at ) => io.putValue( kv._2 )
					}
				case _ => sys.error( "can only use 'set' for an object" )
			}
		}
		
		override def toString =
			io.getType( addr ) match {
				case NULL => "null"
				case FALSE => "false"
				case TRUE => "true"
				case INT => "integer"
				case LONG => "long integer"
				case DOUBLE => "double"
				case NIL => "empty string"
				case STRING => "string"
				case EMPTY => "empty object"
				case OBJECT => "object"
			}
	}
}