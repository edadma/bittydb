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
	private [bittydb] var _root: Long = _
	
	if (io.size == 0) {
		version = VERSION
		io putByteString s"BittyDB $version"
		io.charset = charset
		io putByteString charset.name
		freeListPtr = io.pos
		freeList = 0
		io putBig freeList
		_root = io.pos
		io putByte MEMBERS
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
						_root = io.pos
					case _ => invalid
				}
			case _ => invalid
		}
	
	private def invalid = sys.error( "invalid database" )
	
	def root = new Pointer( _root )
	
	def close = io.close
	
	override def toString = "connection to " + io
	
	class Pointer( addr: Long ) {
		def get = io.getValue( addr )
		
		def put( v: Any ) {
			v match {
				case m: collection.Map[_, _] if addr == _root =>
					io.size = _root + 1
					io.pos = io.size
					io.putObject( m )
				case _ if addr == _root => sys.error( "can only 'put' an object at root" )
				case _ => io.putValue( addr, v )
			}
			
			io.finish
		}
		
		private [bittydb] def lookup( key: Any ): Either[Option[Long], Long] = {
			var where: Option[Long] = None
			
			def chunk: Option[Long] = {
				val cont = io.getBig
				val len = io.getBig
				val start = io.pos
				
				while (io.pos - start < len) {
					val addr = io.pos
					
					if (io.getByte == USED)
						if (io.getValue == key)
							return Some( addr )
						else
							io.skipValue
					else {
						if (where == None)
							where = Some( io.pos - 1 )
							
						io.skipValue
						io.skipValue
					}
				}
				
				if (cont > 0) {
					io.pos = cont
					chunk
				}
				else
					None
			}
			
			io.skipBig
			
			chunk match {
				case Some( addr ) => Right( addr )
				case None => Left( where )
			}
		}
		
		def remove( key: Any ) =
			io.getType( addr ) match {
				case EMPTY => false
				case MEMBERS =>
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
				case MEMBERS =>
					lookup( key ) match {
						case Left( _ ) => None
						case Right( at ) => Some( new Pointer(at + 1 + VWIDTH) )
					}
				case _ => sys.error( "can only use 'find' for an object" )
			}
		
		def key( k: Any ) = find( k ).get
		
		private [bittydb] def ending =
			io.getType( addr ) match {
				case MEMBERS|ELEMENTS =>
					io.getBig match {
						case NUL =>
						case addr => io.pos = addr
					}
					
					io.skipBig
					
					val res = io.pos + BWIDTH + io.getBig == io.size
					
					io.pos -= 2*BWIDTH
					res
				case STRING => sys.error( "not yet" )
				case BIGINT => sys.error( "not yet" )
				case DECIMAL => sys.error( "not yet" )
			}
			
		def set( kv: (Any, Any) ) =
			io.getType( addr ) match {
				case EMPTY => io.putValue( addr, Map(kv) )
				case MEMBERS =>
					val first = io.pos
					
					lookup( kv._1 ) match {
						case Left( None ) =>
							if (ending) {
								io.skipBig
								io.addBig( PWIDTH )
								io.append
								io.putPair( kv )
							}
							else {
								val cont = io.allocComposite
								
								cont.backpatch( io, first )
								cont.putObjectChunk( Map(kv) )
							}
							
							io.finish
							false
						case Left( Some(insertion) ) =>
							io.putPair( insertion, kv )
							io.finish
							false
						case Right( at ) =>
							io.putValue( kv._2 )
							io.finish
							true
					}
				case _ => sys.error( "can only use 'set' for an object" )
			}
		
		def append( elems: Any* ) = appendSeq( elems )
		
		def appendSeq( s: collection.TraversableOnce[Any] ) {
			io.getType( addr ) match {
				case NIL => io.putValue( addr, s )
				case ELEMENTS =>
					val first = io.pos
					
					if (ending) {
						io.skipBig
						
						val sizeptr = io.pos
						
						io.append
						
						var count = 0
						
						for (e <- s) {
							io.putElement( e )
							count += 1
						}
						
						io.addBig( sizeptr, count*EWIDTH )
					}
					else {
						val cont = io.allocComposite
						
						cont.backpatch( io, first )
						cont.putArrayChunk( s )
					}
				case _ => sys.error( "can only use 'append' for an array" )
			}
					
			io.finish
		}
		
		def prepend( elems: Any* ) = prependSeq( elems )
		
		def prependSeq( s: collection.TraversableOnce[Any] ) {		
			io.getType( addr ) match {
				case NIL => io.putValue( addr, s )
				case ELEMENTS =>
						io.skipType( addr )
						
						val cont = io.allocComposite
						
						cont.putArray( s )
					
					io.finish
				case _ => sys.error( "can only use 'prepend' for an array" )
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
				case STRING => "string"
				case NIL => "empty array"
				case ELEMENTS => "array"
				case EMPTY => "empty object"
				case MEMBERS => "object"
			}
	}
}