package xyz.hyperreal.bittydb

import collection.mutable.ListBuffer


class AllocIO( io: IO ) extends MemIO {
	private [bittydb] val backpatches = new ListBuffer[(IO, Long, Long)]
	private [bittydb] var base: Long = _
	
	private [bittydb] lazy val bucket = java.lang.Long.numberOfTrailingZeros(bitCeiling(size) max lowestSize) - sizeShift
	
	charset = io.charset
	bwidth = io.bwidth
	cwidth = io.cwidth
	
	def backpatch( io: IO, src: Long ) =
		backpatches += ((io, src, pos))
	
	def writeBackpatches {
		for ((io, src, target) <- backpatches)
			io.putBig( src, base + target )
	}
}