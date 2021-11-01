package xyz.hyperreal.bittydb

trait IOConstants {
  val NULL = 0x00 // JVM null
  val NSTRING = 0X01 // empty (null) string
  val BOOLEAN = 0x10
  val FALSE: Int = BOOLEAN | 0
  val TRUE: Int = BOOLEAN | 1
  val INTEGER = 0x20
  val BYTE: Int = INTEGER | 0
  val SHORT: Int = INTEGER | 1
  val INT: Int = INTEGER | 2
  val LONG: Int = INTEGER | 3
  val BIGINT = 0x30
  //					0x40	maybe SINGLE (Float)
  val DOUBLE = 0x50
  val DECIMAL = 0x60
  val SSTRING = 0x70 // with length - 1 in low nibble
  val STRING = 0x80 // with encoding included and length size in low nibble
  val UUID = 0x90
  val OBJECT = 0xA0
  val EMPTY: Int = OBJECT | 0
  val MEMBERS: Int = OBJECT | 1
  val ARRAY = 0xB0
  val NIL: Int = ARRAY | 0
  val ELEMENTS: Int = ARRAY | 1
  val POINTER = 0xC0
  val TIMESTAMP = 0xD0
  val DATETIME = 0xE0
  val DELETED = 0xF0

  val UBYTE_LENGTH = 0
  val USHORT_LENGTH = 1
  val INT_LENGTH = 2
  val ENCODING_INCLUDED = 4

  val DATETIME_WIDTH = 16

  val NUL = 0 // null pointer value

  val USED = 0xFF
  val UNUSED = 0xEE
}
