package ca.hyperreal.bittydb

import java.io.File
import java.nio.charset.Charset
import java.nio.charset.StandardCharsets.UTF_8

import util.Either
import collection.{TraversableOnce, AbstractIterator, Map => CMap}


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
	
	val root = new DBFilePointer( _root )
	
	def collection( name: String ) = new Collection( root, name )
	
	def close = io.close
	
	override def toString = "connection to " + io
	
	class Cursor( elem: Long ) extends DBFilePointer( elem + 1 ) {
		def remove = io.putByte( elem, UNUSED )
		
		override def get =
			if (io.getByte( elem ) == UNUSED)
				sys.error( "element has been removed" )
			else
				super.get
	}
	
	class DBFilePointer( protected val addr: Long ) extends Pointer
	
	abstract class Pointer extends (Any => Pointer) {
		protected def addr: Long
		
		def collection( name: String ) = new Collection( this, name )
		
		def apply( k: Any ) =
			key( k ) match {
				case None =>
					new Pointer {
						def addr = sys.error( "invalid pointer" )
						
						override def ===( a: Any ) = false
					}
				case Some( p ) => p
			}
		
		def getAs[A] = get.asInstanceOf[A]
		
		def get = io.getValue( addr )
		
		def ===( a: Any ) = get == a
		
		def put( v: Any ) {
			v match {
				case m: CMap[_, _] if addr == _root =>
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
		
		def key( k: Any ): Option[DBFilePointer] =
			io.getType( addr ) match {
				case MEMBERS =>
					lookup( k ) match {
						case Left( _ ) => None
						case Right( at ) => Some( new DBFilePointer(at + 1 + VWIDTH) )
					}
				case _ => None
			}
		
		private [bittydb] def ending =
			io.getType( addr ) match {
				case t@(MEMBERS|ELEMENTS) =>
					if (t == ELEMENTS)
						io.skipBig
					
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
							} else {
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
		
		def appendSeq( s: TraversableOnce[Any] ) {
			io.getType( addr ) match {
				case NIL => io.putValue( addr, s )
				case ELEMENTS =>
					val header = io.pos
					
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
					} else {
						io.inert {
							if (io.getBig( header ) == NUL)
								io.putBig( header, header + 2*BWIDTH )
						}
						
						val cont = io.allocComposite
						
						cont.backpatch( io, header + BWIDTH )
						cont.putArrayChunk( s )
					}
				case _ => sys.error( "can only use 'append' for an array" )
			}
			
			io.finish
		}
		
		def prepend( elems: Any* ) = prependSeq( elems )
		
		def prependSeq( s: TraversableOnce[Any] ) {
			io.getType( addr ) match {
				case NIL => io.putValue( addr, s )
				case ELEMENTS =>
					val header = io.pos
					
					val first = io.getBig match {
						case NUL => header + 2*BWIDTH
						case p => p
					}
					
					if (io.getBig == NUL)
						io.putBig( io.pos - BWIDTH, header + 2*BWIDTH )
						
					io.pos = header
					
					val cont = io.allocComposite
					
					cont.putArrayChunk( s, first )
				case _ => sys.error( "can only use 'prepend' for an array" )
			}
			
			io.finish
		}
		
		def membersAs[A] = members.asInstanceOf[Iterator[A]]
		
		def members = iterator map (_.get)
		
		def at( index: Int ) = iterator drop (index) next
		
		def iterator = {
			io.getType( addr ) match {
				case NIL => Iterator.empty
				case ELEMENTS =>
					val header = io.pos
					val first = io.getBig match {
						case NUL => header + 2*BWIDTH
						case p => p
					}
					
					new AbstractIterator[Cursor] {
						var cont: Long = _
						var chunksize: Long = _
						var cur: Long = _
						var scan = false
						var done = false
						
						chunk( first )
						
						private def chunk( p: Long ) {
							cont = io.getBig( p )
							chunksize = io.getBig
							cur = p + 2*BWIDTH
						}
						
						def hasNext = {
							def advance = {
								chunksize -= EWIDTH
								
								if (chunksize == 0)
									if (cont == NUL) {
										done = true
										false
									} else {
										chunk( cont )
										true
									}
								else {
									cur += EWIDTH
									true
								}
							}

							def nextused: Boolean =
								if (advance)
									if (io.getByte( cur ) == USED)
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
								new Cursor( cur )
							} else
								throw new java.util.NoSuchElementException( "next on empty iterator" )
					}
				case _ => sys.error( "can only use 'iterator' for an array" )
			}
		}
		
		def kind = io.getType( addr )
		
		override def toString =
			kind match {
				case NULL => "null"
				case FALSE => "false"
				case TRUE => "true"
				case BYTE => "byte"
				case SHORT => "short integer"
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