package ca.hyperreal.bittydb

import java.io.File
import java.nio.charset.Charset
import java.util.NoSuchElementException

import util.Either
import collection.{TraversableOnce, AbstractIterator, Map => CMap}
import collection.mutable.{HashMap, AbstractMap}

import ca.hyperreal.lia.Math


object Connection {
	private def invalid = throw new InvalidDatabaseException
	
	def disk( file: String, options: (Symbol, Any)* ): Connection = disk( new File(file), options: _* )
	
	def disk( f: File, options: (Symbol, Any)* ) = {
		if (Database.exists( f ) && f.length == 0)
			invalid
		else
			new Connection( new DiskIO(f), options )
	}
	
	def temp( options: (Symbol, Any)* ) = {
		val file = File.createTempFile( "temp", ".bittydb" )
		
		(file, new Connection( new DiskIO(file), options ))
	}
	
	def mem( options: (Symbol, Any)* ) = new Connection( new MemIO, options )
}

class InvalidDatabaseException extends Exception( "invalid database" )

class Connection( private [bittydb] val io: IO, options: Seq[(Symbol, Any)] ) extends AbstractMap[String, Collection] with IOConstants {
	private [bittydb] var version: String = _
	private [bittydb] var rootPtr: Long = _
	
	private [bittydb] var uuidOption = true
	
	if (io.size == 0) {
		version = VERSION
		io putByteString "BittyDB"
		io putByteString version
		
		val optMap = HashMap[Symbol, Any]( options: _* )
		
		for (opt <- Seq('charset, 'bwidth, 'cwidth, 'uuid) if optMap contains opt) {
			(opt, optMap(opt)) match {
				case ('charset, cs: String) =>
					io.charset = Charset.forName( cs )
				case ('bwidth, n: Int) =>
					if (1 <= n && n <= 8)
						io.bwidth = n
					else
						sys.error( "'bwidth' is between 1 and 8 (inclusive)" )
				case ('cwidth, n: Int) =>
					if (io.bwidth <= n && n <= 255)
						io.cwidth = n
					else
						sys.error( "'cwidth' is at between 'bwidth' and 255 (inclusive)" )
				case ('uuid, on: Boolean) => uuidOption = on
			}
			
			optMap -= opt
		}

		optMap.keys.headOption match {
			case None =>
			case Some( k ) => sys.error( s"invalid option: '$k'" )
		}
		
		io putByteString io.charset.name
		io putByte io.bwidth
		io putByte io.cwidth
		io putBoolean uuidOption
		
		io.bucketsPtr = io.pos
		io.buckets = Array.fill( io.bucketLen )( NUL )
		for (b <- io.buckets) io putBig b
		rootPtr = io.pos
		io putValue( Map.empty )
		io.force
	}
	else
		io.getByteString match {
			case Some( s ) if s == "BittyDB" =>
				io.getByteString match {
					case Some( v ) => version = v
					case _ => Connection.invalid
				}
				
				if (version > VERSION)
					sys.error( "attempting to read database of newer format version" )
					
				io.getByteString match {
					case Some( cs ) => io.charset = Charset.forName( cs )
					case _ => Connection.invalid
				}
				
				io.getByte match {
					case n if 1 <= n && n <= 8 => io.bwidth = n
					case _ => Connection.invalid
				}
				
				io.getUnsignedByte match {
					case n if io.bwidth <= n && n <= 255 => io.cwidth = n
					case _ => Connection.invalid
				}
				
				uuidOption = io.getBoolean
				
				io.bucketsPtr = io.pos
				io.buckets = (for (_ <- 1 to io.bucketLen) yield io.getBig).toArray
				rootPtr = io.pos
			case _ => Connection.invalid
		}
	
	val root = new DBFilePointer( rootPtr )

//	def apply( name: String ) = new Collection( root, name )
	
	def close = io.close

	//
	// Map methods
	//
	
	def get( key: String ): Option[Collection] = if (root.key( key ) == None) None else Some( default(key) )
	
	def iterator: Iterator[(String, Collection)] =
		io.objectIterator( rootPtr ) map {
			a =>
				val name = io.getValue( a + 1 ).asInstanceOf[String]
				
				name -> new Collection( root, name )
		}
	
	def += (kv: (String, Collection)) = sys.error( "use 'set'" )
	
	def -= (key: String) = {
		remove( key )
		this
	}
	
	override def default( name: String ) = new Collection( root, name )
	
	override def toString = "connection to " + io
	
	class Cursor( val elem: Long ) extends DBFilePointer( elem + 1 ) {
		def remove = {
			io.putByte( elem, UNUSED )
			io.remove( elem + 1 )
		}

		override def get =
			if (io.getByte( elem ) == UNUSED)
				sys.error( "element has been removed" )
			else
				super.get
	}
	
	class DBFilePointer( protected val addr: Long ) extends Pointer
	
	abstract class Pointer extends (Any => Pointer) {
		protected def addr: Long
		private [bittydb] val connection = Connection.this
		
		def collection( name: String ) = new Collection( this, name )
		
		def apply( k: Any ) =
			key( k ) match {
				case None =>
					new Pointer {
						def addr = sys.error( "invalid pointer" )
		
						override def !==( a: Any ) = get != a
						
						override def <( a: Any ) = false
						
						override def >( a: Any ) = false
						
						override def <=( a: Any ) = false
						
						override def >=( a: Any ) = false
						
						override def in( s: Set[Any] ) = false
						
						override def nin( s: Set[Any] ) = false
						
						override def ===( a: Any ) = false
					}
				case Some( p ) => p
			}
		
		def getAs[A] = get.asInstanceOf[A]
		
		def get = io.getValue( addr )
		
		def ===( a: Any ) = get == a
		
		def !==( a: Any ) = get != a
		
		def <( a: Any ) = Math.predicate( '<, get, a )
		
		def >( a: Any ) = Math.predicate( '>, get, a )
		
		def <=( a: Any ) = Math.predicate( '<=, get, a )
		
		def >=( a: Any ) = Math.predicate( '>=, get, a )
		
		def in( s: Set[Any] ) = s( get )
		
		def nin( s: Set[Any] ) = !s( get )
		
		def put( v: Any ) {
			v match {
				case m: CMap[_, _] if addr == rootPtr =>
					io.size = rootPtr + 1
					io.pos = io.size
					io.putObject( m )
				case _ if addr == rootPtr => sys.error( "can only 'put' an object at root" )
				case _ => io.putValue( addr, v )
			}
			
			io.finish
		}
	
	def list = io.objectIterator( addr ) map {a => io.getValue( a + 1 ) -> new DBFilePointer( io.pos )} toList
		
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
				
				if (cont != NUL) {
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
							io.remove( at + 1 )
							io.remove( at + 1 + io.vwidth )
							true
					}
				case _ => sys.error( "can only use 'remove' for an object" )
			}
		
		def key( k: Any ): Option[DBFilePointer] =
			io.getType( addr ) match {
				case MEMBERS =>
					lookup( k ) match {
						case Left( _ ) => None
						case Right( at ) => Some( new DBFilePointer(at + 1 + io.vwidth) )
					}
				case _ => None
			}
		
		private [bittydb] def ending =
			io.getType( addr ) match {
				case t@(MEMBERS|ELEMENTS) =>
					if (t == ELEMENTS) {
						io.skipBig
						io.skipBig
					}
					
					io.getBig match {
						case NUL =>
						case addr => io.pos = addr
					}
						
					io.skipBig
					
					val res = io.pos + io.bwidth + io.getBig == io.size
					
					io.pos -= 2*io.bwidth
					res
				case STRING => sys.error( "not yet" )
				case BIGINT => sys.error( "not yet" )
				case DECIMAL => sys.error( "not yet" )
			}
			
		def set( kv: (Any, Any) ) =
			io.getType( addr ) match {
				case EMPTY if addr == rootPtr =>
					io.putByte( addr, MEMBERS )
					io.putObject( Map(kv) )
					io.finish
					false
				case EMPTY =>
					io.putValue( addr, Map(kv) )
					io.finish
					false
				case MEMBERS =>
					val first = io.pos
					
					lookup( kv._1 ) match {
						case Left( None ) =>
// 							if (ending) {
// 								io.skipBig
// 								io.addBig( io.pwidth )
// 								io.append
// 								io.putPair( kv )
// 							} else {
								io.getBig( first ) match {
									case NUL =>
									case last => io.pos = last
								}
								
								val cont = io.allocPad
								
								cont.backpatch( io, first )
								cont.putObjectChunk( Map(kv) )
//							}
							
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

					io.skipBig
					
					if (ending) {
						io.skipBig
						
						val sizeptr = io.pos
						
						io.append
						
						var count = 0L
						
						for (e <- s) {
							io.putElement( e )
							count += 1
						}
						
						io.addBig( sizeptr, count*io.ewidth )
						io.addBig( header, count )
					} else {
						io.inert {
							if (io.getBig( header + io.bwidth ) == NUL)
								io.putBig( header + io.bwidth, header + 3*io.bwidth )
						}
						
						val cont = io.allocPad
						
						cont.backpatch( io, header + 2*io.bwidth )
						cont.putArrayChunk( s, io, header )
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
					
					io.skipBig
					
					val first = io.getBig match {
						case NUL => header + 3*io.bwidth
						case p => p
					}
					
					if (io.getBig == NUL)
						io.putBig( io.pos - io.bwidth, header + 3*io.bwidth )
						
					io.pos = header + io.bwidth
					
					val cont = io.allocPad
					
					cont.putArrayChunk( s, io, header, first )
				case _ => sys.error( "can only use 'prepend' for an array" )
			}
			
			io.finish
		}
		
		def length =
			io.getType( addr ) match {
				case NIL => 0L
				case ELEMENTS => io.getBig
				case _ => sys.error( "can only use 'length' for an array" )
			}
		
		def cursor = io.arrayIterator( addr ) map (new Cursor( _ ))
		
		def membersAs[A] = members.asInstanceOf[Iterator[A]]
		
		def members = cursor map (_.get)
		
		def at( index: Int ) = new Cursor( io.arrayIterator(addr) drop (index) next )
		
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