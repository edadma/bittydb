package ca.hyperreal

//import org.slf4j.LoggerFactory
import java.sql.Timestamp


package object bittydb {
//	val logger = LoggerFactory.getLogger( classOf[Connection] )
	
	val VERSION = "1"
	
	val logging = true
	
	def log( o: AnyRef ) {
		if (logging)
			println( "[log]  " + o.toString )
	}
	
	def timestamp = new Timestamp(System.currentTimeMillis)
}