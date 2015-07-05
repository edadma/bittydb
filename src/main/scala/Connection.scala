package ca.hyperreal.bittydb

import java.io.File


object Connection
{
	def disk( file: String ): Connection = disk( file )
	
	def disk( f: File ) = new Connection( new DiskIO(f) )
	
	
}

class Connection( io: IO )
{
	io.getOptionString match
	{
		case None => invalid
		case Some( s ) =>
			
	}
	
	def invalid = sys.error( "invalid database" )
}