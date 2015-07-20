package ca.hyperreal.bittydb

import java.nio.charset.Charset

import collection.mutable.ListBuffer


class AllocIO( cs: Charset, bw: Int, cw: Int ) extends MemIO {
	private [bittydb] val backpatches = new ListBuffer[(IO, Long, Long)]
	
	charset = cs
	bwidth = bw
	cwidth = cw
	
	def backpatch( io: IO, src: Long ) =
		backpatches += ((io, src, pos))

	def totalSize: Long = size + allocSize
	
	def writeBackpatches( base: Long ) {
		for ((io, src, target) <- backpatches)
			io.putBig( src, base + target )
	}
}