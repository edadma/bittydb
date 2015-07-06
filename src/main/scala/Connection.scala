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

class Connection( private [bittydb] val io: IO, charset: Charset )
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
		io putValue Map.empty
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
		def get = {
			io.pos = addr
			io.getValue
		}
	}
}