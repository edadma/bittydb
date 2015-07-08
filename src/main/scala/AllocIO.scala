package ca.hyperreal.bittydb

import collection.mutable.ListBuffer


class AllocIO extends MemIO {
	private [bittydb] val backpatches = new ListBuffer[(IO, Long, Long)]
		
	def backpatch( io: IO, src: Long ) =
		backpatches += ((io, src, pos))

	def totalSize: Long = size + allocSize
	
	def writeBackpatches( dest: IO, base: Long ) {
		for ((io, src, target) <- backpatches)
			io.putBig( src, base + target )
			
//		allocs foreach (_.writeBackpatches( base + size ))
	}
}