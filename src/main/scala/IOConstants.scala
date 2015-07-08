package ca.hyperreal.bittydb


trait IOConstants {
	val NULL =			0x00
	val BOOLEAN =		0x10
	val FALSE =			BOOLEAN|0
	val TRUE =			BOOLEAN|1
	val INT =			0x20
	val LONG =			0x30
	val BIGINT =		0x40
	val DOUBLE =		0x50
	val DECIMAL =		0x60
	val SSTRING =		0x70	// with length in low nibble
	val STRING =		0x80
	val ESTRING =		0x90
	val OBJECT =		0xA0
	val EMPTY =			OBJECT|0
	val MEMBERS =		OBJECT|1
	val ARRAY =			0xB0
	val NIL =			ARRAY|0
	val ELEMENTS =		ARRAY|1
	val POINTER =		0xC0
	val TIMESTAMP =		0xD0
	val UUID =			0xE0
	val TYPE =			0xF0
	
	val BWIDTH = 5				// big (i.e. pointers, sizes) width
	val VWIDTH = 1 + 8			// value width
	val PWIDTH = 1 + 2*VWIDTH 	// pair width
	
	val USED = 0
	val UNUSED = 1
}
