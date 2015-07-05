package ca.hyperreal

//import org.slf4j.LoggerFactory


package object bittydb {
//	val logger = LoggerFactory.getLogger( classOf[Connection] )
	
	val VERSION = "1"
	
	val logging = true
	
	def log( o: AnyRef ) {
		if (logging)
			println( "[log]  " + o.toString )
	}
}