package ca.hyperreal.bittydb

import java.io.File
import java.nio.charset.Charset
import java.util.NoSuchElementException

import util.Either
import collection.{TraversableOnce, AbstractIterator, Map => CMap}
import collection.mutable.{HashMap, AbstractMap}

import ca.hyperreal.lia.Math


object Connection
{
	def disk( file: String, options: (Symbol, Any)* ): Connection = disk( new File(file), options: _* )
	
	def disk( f: File, options: (Symbol, Any)* ) = new Connection( new DiskIO(f), options )
	
	def mem( options: (Symbol, Any)* ) = new Connection( new MemIO, options )
}

class Connection( private [bittydb] val io: IO, options: Seq[(Symbol, Any)] ) extends AbstractMap[String, Collection] with IOConstants
{
	private [bittydb] var version: String = _
	private [bittydb] var freeListPtr: Long = _
	private [bittydb] var freeList: Long = _
	private [bittydb] var _root: Long = _
	
	if (io.size == 0) {
		version = VERSION
		io putByteString "BittyDB"
		io putByteString version
		
		val optMap = HashMap[Symbol, Any]( options: _* )
		
		for (opt <- Seq('charset, 'bwidth, 'cwidth) if optMap contains opt) {
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
			case Some( s ) if s == "BittyDB" =>
				log( s )
				
				io.getByteString match {
					case Some( v ) => version = v
					case _ => invalid
				}
				
				if (version > VERSION)
					sys.error( "attempting to read database of newer format version" )
					
				io.getByteString match {
					case Some( cs ) => io.charset = Charset.forName( cs )
					case _ => invalid
				}
				
				io.getByte match {
					case n if 1 <= n && n <= 8 => io.bwidth = n
					case _ => invalid
				}
				
				io.getUnsignedByte match {
					case n if io.bwidth <= n && n <= 255 => io.cwidth = n
					case _ => invalid
				}
				
				freeListPtr = io.pos
				freeList = io.getBig
				_root = io.pos
			case _ => invalid
		}
	
	private def invalid = sys.error( "invalid database" )
	
	val root = new DBFilePointer( _root )

//	def apply( name: String ) = new Collection( root, name )
	
	def close = io.close

	//
	// Map methods
	//
	
	def get( key: String ): Option[Collection] = if (root.key( key ) == None) None else Some( default(key) )
	
	def iterator: Iterator[(String, Collection)] =
		root.objectIterator map {
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
		def remove = io.putByte( elem, UNUSED )
		
		override def get =
			if (io.getByte( elem ) == UNUSED)
				sys.error( "element has been removed" )
			else
				super.get
	}
	
	class DBFilePointer( private [bittydb] val addr: Long ) extends Pointer
	
	abstract class Pointer extends (Any => Pointer) {
		private [bittydb] def addr: Long
		
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
				case m: CMap[_, _] if addr == _root =>
					io.size = _root + 1
					io.pos = io.size
					io.putObject( m )
				case _ if addr == _root => sys.error( "can only 'put' an object at root" )
				case _ => io.putValue( addr, v )
			}
			
			io.finish
		}
	
	def list = objectIterator map {a => io.getValue( a + 1 ) -> new DBFilePointer( io.pos )} toList
	
	def objectIterator: Iterator[Long] =
		io.getType( addr ) match {
			case EMPTY => Iterator.empty
			case MEMBERS =>
				val header = io.pos
				
				new AbstractIterator[Long] {
					var cont: Long = _
					var chunksize: Long = _
					var cur: Long = _
					var scan = false
					var done = false
					
					chunk( header + io.bwidth )
					
					private def chunk( p: Long ) {
						cont = io.getBig( p )
						chunksize = io.getBig
						cur = io.pos
					}
					
					def hasNext = {
						def advance = {
							chunksize -= io.pwidth
							
							if (chunksize == 0)
								if (cont == NUL) {
									done = true
									false
								} else {
									chunk( cont )
									true
								}
							else {
								cur += io.pwidth
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
							cur
						} else
							throw new NoSuchElementException( "next on empty objectIterator" )
				}
			case _ => sys.error( "can only use 'objectIterator' for an object" )
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
				case EMPTY => io.putValue( addr, Map(kv) )
				case MEMBERS =>
					val first = io.pos
					
					lookup( kv._1 ) match {
						case Left( None ) =>
							if (ending) {
								io.skipBig
								io.addBig( io.pwidth )
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
						
						val cont = io.allocComposite
						
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
					
					val cont = io.allocComposite
					
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
		
		def membersAs[A] = members.asInstanceOf[Iterator[A]]
		
		def members = arrayIterator map (_.get)
		
		def at( index: Int ) = arrayIterator drop (index) next
		
		def arrayIterator = {
			io.getType( addr ) match {
				case NIL => Iterator.empty
				case ELEMENTS =>
					val header = io.pos
					
					io.skipBig
					
					val first = io.getBig match {
						case NUL => header + 3*io.bwidth
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
							cur = io.pos
						}
						
						def hasNext = {
							def advance = {
								chunksize -= io.ewidth
								
								if (chunksize == 0)
									if (cont == NUL) {
										done = true
										false
									} else {
										chunk( cont )
										true
									}
								else {
									cur += io.ewidth
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
								throw new NoSuchElementException( "next on empty arrayIterator" )
					}
				case _ => sys.error( "can only use 'arrayIterator' for an array" )
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