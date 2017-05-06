package xyz.hyperreal.bittydb

import collection.mutable.ListBuffer


class AllocIO( io: IO ) extends MemIO {
	private [bittydb] val backpatches = new ListBuffer[(IO, Long, Long)]
	private [bittydb] var base: Long = _
	
	private [bittydb] lazy val bucket = java.lang.Long.numberOfTrailingZeros( allocSize ) - sizeShift
	
	charset = io.charset
	pwidth = io.pwidth
	cwidth = io.cwidth
	bucketsPtr = io.bucketsPtr

	def backpatch( io: IO, src: Long ) =
		backpatches += ((io, src, pos))
	
	def writeBackpatches {
		for ((io, src, target) <- backpatches)
			io.putBig( src, base + target )
	}

	private [bittydb] def allocSize = bitCeiling( size + 1 ) max minblocksize

	override def toString = f"[alloc: base = $base%x, size = $size%x]"
}