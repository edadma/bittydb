BittyDB File Format
===================

Types
-----

- NULL =			0x00
- BOOLEAN =		0x10
- FALSE =			BOOLEAN|0
- TRUE =			BOOLEAN|1
- INTEGER =		0x20
- BYTE =			INTEGER|0
- SHORT =			INTEGER|1
- INT =			INTEGER|2
- LONG =			INTEGER|3
- BIGINT =		0x30
- DOUBLE =		0x50
- DECIMAL =		0x60
- SSTRING =		0x70	// with length - 1 in low nibble
- STRING =		0x80	// with encoding included and length size in low nibble
- UUID =			0x90
- OBJECT =		0xA0
- EMPTY =			OBJECT|0
- MEMBERS =		OBJECT|1
- ARRAY =			0xB0
- NIL =			ARRAY|0
- ELEMENTS =		ARRAY|1
- POINTER =		0xC0
- TIMESTAMP =		0xD0
- DATETIME =		0xE0
	
- UBYTE_LENGTH = 0
- USHORT_LENGTH = 1
- INT_LENGTH = 2
- WITH_ENCODING = 4
- SSTRING_MAX = 16


Header
------

Field          | Contents         | Default | Description
-----          | --------         | ------- | -----------
identifier     | `0x07 BittyDB`   |         | 8 byte (64 bit) "magic number" equal to 523096455619626050 when viewed as a big-endian number, serving to identify the file type
version        | <n> <version>    |         | format version: *n* is the number of bytes in the *version* number string
charset        | <n> <charset>    | `UTF-8` | character set for strings: *n* is the number of bytes in the *charset* name string
bwidth         | <n>              | `0x05`  | *n* is the width of "big" numbers in the database which are used for file pointers and chunk sizes
cwidth         | <n>              | `0x08`  | *n* is the width of cells in the database which are the basic data "containers"

The header is followed immediately by the *root* object.  All other values in the database can have any type, but the root must be an object.

Objects
-------

Field          | Contents         | Default | Description
-----          | --------         | ------- | -----------
type           |                  |         | 

