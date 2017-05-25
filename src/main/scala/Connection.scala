package xyz.hyperreal.bittydb

import java.io.File
import java.nio.charset.Charset

import util.Either
import collection.{LinearSeq, Map => CMap}
import collection.mutable.AbstractMap
import xyz.hyperreal.lia.Math


object Connection {
	private def invalid = {
		throw new InvalidDatabaseException
	}
	
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

	if (io.size == 0) {
		version = FORMAT_VERSION
		io putByteString "BittyDB"
		io putByteString version

		for (opt <- options)
			opt match {
				case ('charset, cs: String) =>
					io.charset = Charset.forName( cs )
				case ('pwidth, n: Int) =>
					if (1 <= n && n <= 8)
						io.pwidth = n
					else
						sys.error( "'pwidth' is between 1 and 8 (inclusive)" )
				case ('cwidth, n: Int) =>
					if (io.pwidth <= n && n <= 255)
						io.cwidth = n
					else
						sys.error( "'cwidth' is between 'pwidth' and 255 (inclusive)" )
				case ('uuid, on: Boolean) => io.uuidOption = on
				case (Symbol( o ), _) => sys.error( s"unknown option '$o'" )
			}

		io putByteString io.charset.name
		io putByte io.pwidth
		io putByte io.cwidth
		io putBoolean io.uuidOption
		io.bucketsPtr = io.pos
		io.buckets = Array.fill( io.bucketLen )( NUL )

		for (b <- io.buckets)
			io putBig b

		rootPtr = io.pos
		io putValue Map.empty
		io.force
	}
	else
		io.getByteString match {
			case Some( s ) if s == "BittyDB" =>
				io.getByteString match {
					case Some( v ) => version = v
					case _ => io.check
				}
				
				if (version > FORMAT_VERSION)
					sys.error( "attempting to read database of newer format version" )
					
				io.getByteString match {
					case Some( cs ) => io.charset = Charset.forName( cs )
					case _ => io.check
				}
				
				io.getByte match {
					case n if 1 <= n && n <= 8 => io.pwidth = n
					case _ => io.check
				}
				
				io.getUnsignedByte match {
					case n if io.pwidth <= n && n <= 255 => io.cwidth = n
					case _ => io.check
				}
				
				io.uuidOption = io.getBoolean
				
				io.bucketsPtr = io.pos
				io.buckets = (for (_ <- 1 to io.bucketLen) yield io.getBig).toArray
				rootPtr = io.pos
			case _ => io.check
		}
	
	val root = new DBFilePointer( rootPtr )

//	def apply( name: String ) = new Collection( root, name )
	
	def close = io.close

	def length = io.size

	//
	// Map methods
	//
	
	def get( key: String ): Option[Collection] = if (root.key(key).isEmpty) None else Some( default(key) )
	
	def iterator: Iterator[(String, Collection)] =
		io.listObjectIterator( rootPtr ) map {
			case (k, _) =>
				val name = io.getValue( k ).asInstanceOf[String]

				name -> new Collection( root, name )
		}
	
	def += (kv: (String, Collection)) = sys.error( "use 'set'" )
	
	def -= (key: String) = {
		remove( key )
		this
	}
	
	override def default( name: String ) = new Collection( root, name )
	
	override def toString = "connection to " + io
	
	class Cursor( list: Long, val chunk: Long, val elem: Long ) extends DBFilePointer( elem ) {
		lazy val (freeptr, lenptr) =
			io.getType( list ) match {
				case NIL => sys.error( "can't have a cursor in an empty list" )
				case LIST_ELEMS => (io.pos + io.pwidth, io.pos + 2*io.pwidth)
				case t => sys.error( f"can only get a cursor for an list: $t%x, ${io.pos}%x" )
			}

		def remove = {
			val nextptr =
				io.getBig( chunk + 2*io.pwidth ) match {
					case NUL =>
						io.putBig( chunk + io.pwidth, io.getBig(freeptr) )
						io.putBig( freeptr, chunk )
						NUL
					case p => p
				}

			io.putBig( chunk + 2*io.pwidth, elem )
			io.addBig( lenptr, -1 )
			io.addBig( chunk + 4*io.pwidth, -1 )
			io.remove( elem )
			io.putByte( elem, DELETED )
			io.putBig( nextptr )
		}

		override def get =
			if (io.peekUnsignedByte( elem ) == DELETED)
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
		
						override def =!=( a: Any ) = false
						
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
		
		def =!=( a: Any ) = get != a
		
		def <( a: Any ) = Math.predicate( '<, get, a )
		
		def >( a: Any ) = Math.predicate( '>, get, a )
		
		def <=( a: Any ) = Math.predicate( '<=, get, a )
		
		def >=( a: Any ) = Math.predicate( '>=, get, a )
		
		def in( s: Set[Any] ) = s( get )
		
		def nin( s: Set[Any] ) = !s( get )

		// todo: this looks wrong: "io.remove( addr )" won't work right: discard this method maybe
		def put( v: Any ) {
			v match {
				case m: CMap[_, _] if addr == rootPtr && m.isEmpty =>
					io.size = rootPtr
					io.pos = io.size
					io putValue Map.empty
				case m: CMap[_, _] if addr == rootPtr =>
					io.size = rootPtr
					io.pos = io.size
					io.putByte( addr, LIST_MEMS )
					io.putListObject( m )
				case _ if addr == rootPtr => sys.error( "can only 'put' an object at root" )
				case _ =>
					io.remove( addr )
					io.putValue( addr, v )
			}
			
			io.finish
		}
	
//		def list = io.objectIterator( addr ) map {a => io.getValue( a + 1 ) -> new DBFilePointer( io.pos )} toList	//todo: recode
		
		private [bittydb] def lookup( key: Any ): Option[Long] = {
			for ((k, v) <- io.listObjectIterator( addr ))
				if (key == io.getValue( k ))
					return Some( v )

			None
		}

		// todo: this isn't correct: use code from other remove() method - needs list, chunk and element addresses: remove elements in reverse order so set() works right
		def remove( key: Any ) =
			lookup( key ) match {
				case None => false
				case Some( at ) =>
					io.remove( at + io.vwidth )
					io.remove( at )
					true
			}

		def key( k: Any ): Option[DBFilePointer] = lookup( k ) map (new DBFilePointer(_))

		def set( kv: (Any, Any) ) =
			io.getType( addr ) match {
				case EMPTY if addr == rootPtr =>
					io.putByte( addr, LIST_MEMS )
					io.putListObject( Map(kv) )
					io.finish
					false
				case EMPTY =>
					io.putValue( addr, Map(kv) )
					io.finish
					false
				case LIST_MEMS =>
					lookup( kv._1 ) match {
						case None =>
							// todo: care needs to be taken in case reclaimed storage is available which gets unfreed in reverse order
							insert( kv._1 )
							insert( kv._2 )
							false
						case Some( addr ) =>
							io.remove( addr )
							io.putValue( addr, kv._2 )
							io.finish
							true
					}
				case _ => sys.error( "can only use 'set' for an object" )
			}

		def insert( elem: Any ): Unit = {
			io.getType( addr ) match {
				case NIL => append( elem )
				case LIST_ELEMS|LIST_MEMS =>
					io.skipBig	// skip last chunk pointer

					val freeptr = io.pos

					io.getBig match {
						case NUL => append( elem )
						case chunk =>
							val countptr = io.pos
							val slot = io.getBig( chunk + 2*io.pwidth )
							val nextfree = io.getBig( slot + 1 )

							io.putBig( chunk + 2*io.pwidth, nextfree )

							if (nextfree == NUL)
								io.putBig( freeptr, io.getBig(chunk + io.pwidth) )

							io.putValue( slot, elem )
							io.finish
							io.addBig( chunk + 4*io.pwidth, 1 )
							io.addBig( countptr, 1 )
					}
				case _ => sys.error( "can only use 'insert' for a list" )
			}
		}

		def append( elems: Any* ) = appendSeq( elems.toList )
		
		def appendSeq( s: LinearSeq[Any] ) {
			io.getType( addr ) match {
				case NIL => io.putValue( addr, s )
				case LIST_ELEMS|LIST_MEMS =>
					val header = io.pos

					io.getBig match {
						case NUL =>
							io.skipBig		// skip free pointer
							io.skipBig		// skip count
						case last => io.pos = last
					}

					val cont = io.alloc

					cont.backpatch( io, header )
					cont.putListChunk( s, io, header + 2*io.pwidth )
				case _ => sys.error( "can only use 'append' for a list" )
			}
			
			io.finish
		}

		def length =
			io.getType( addr ) match {
				case NIL|EMPTY_ARRAY|EMPTY => 0L
				case LIST_ELEMS => io.getBig( io.pos + 2*io.pwidth )
				case LIST_MEMS => io.getBig( io.pos + 2*io.pwidth )/2
				case ARRAY_ELEMS => io.getBig
				case ARRAY_MEMS => io.getBig/2
				case _ => sys.error( "can only use 'length' for an array or a list" )
			}
		
		def cursor = io listIterator addr map {case (chunk, elem) => new Cursor(addr, chunk, elem)}
		
		def elementsAs[A] = elements.asInstanceOf[Iterator[A]]

		def elements = cursor map (_ get)
		
		def at( index: Int ) = {
			val (chunk, elem) = io listIterator addr drop index next

			new Cursor(addr, chunk, elem)
		}
		
		def typ = io getType addr

		def isMap =
			typ match {
				case EMPTY|LIST_MEMS|ARRAY_MEMS => true
				case _ => false
			}

		override def toString =
			typ match {
				case NULL => "null"
				case FALSE => "false"
				case TRUE => "true"
				case BYTE => "byte"
				case SHORT => "short integer"
				case INT => "integer"
				case LONG => "long integer"
				case DOUBLE => "double"
				case STRING => "string"
				case NIL => "empty list"
				case ARRAY_ELEMS => "array"
				case ARRAY_MEMS => "array object"
				case LIST_ELEMS => "list"
				case LIST_MEMS => "list object"
				case EMPTY => "empty object"
			}
	}
}