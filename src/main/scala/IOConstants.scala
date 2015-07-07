package ca.hyperreal.bittydb


trait IOConstants {
	val POINTER = -1
	val NULL = 0
	val TRUE = 1
	val FALSE = 2
	val INT = 3
	val LONG = 4
	val BIGINT = 5
	val DOUBLE = 6
	val DECIMAL = 7
	val STRING = 8
	val EMPTY = 9
	val OBJECT = 10
	val NIL = 11
	val ARRAY = 12
	
	val BWIDTH = 5				// big (i.e. pointers, sizes) width
	val VWIDTH = 1 + 8			// value width
	val PWIDTH = 1 + 2*VWIDTH 	// pair width
	
	val USED = 0
	val UNUSED = 1
}
