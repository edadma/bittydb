package ca.hyperreal.bittydb

import java.io.File


object Connection
{
	def disk( file: String ): Connection = disk( new File(file) )
	
	def disk( f: File ) = new Connection( new DiskIO(f) )
	
	
}

class Connection( io: IO )
{
	io.getOptionString match
	{
		case Some( s ) if s startsWith "BittyDB " =>
			log( s )
			
			val v = s substring 8
			
			if (v > VERSION)
				sys.error( "attempting to read database of newer format version" )
		case None => invalid
	}
	
	def invalid = sys.error( "invalid database" )
}