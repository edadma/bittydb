package ca.hyperreal.bittydb

import java.nio.charset.Charset

import collection.mutable.ListBuffer


class AllocIO( cs: Charset, bw: Int, cw: Int ) extends MemIO {
	private [bittydb] val backpatches = new ListBuffer[(IO, Long, Long)]
	private [bittydb] var base: Long = _
	
	charset = cs
	bwidth = bw
	cwidth = cw
	
	def backpatch( io: IO, src: Long ) =
		backpatches += ((io, src, pos))
	
	def writeBackpatches {
		for ((io, src, target) <- backpatches)
			io.putBig( src, base + target )
	}
}