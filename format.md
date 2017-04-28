BittyDB File Format
===================

Internal Types
--------------

- NULL = 0x00
- NSTRING = 0x01
- BOOLEAN = 0x10
	- FALSE = BOOLEAN|0
	- TRUE = BOOLEAN|1
- INTEGER = 0x20
	- BYTE = INTEGER|0
	- SHORT = INTEGER|1
	- INT = INTEGER|2
	- LONG = INTEGER|3
- BIGINT = 0x30
- DOUBLE = 0x50
- DECIMAL = 0x60
- SSTRING = 0x70 (with length - 1 in low nibble)
- STRING = 0x80 (with encoding included and length type in low nibble)
	options "or'd" in:
	- UBYTE_LENGTH = 0
	- USHORT_LENGTH = 1
	- INT_LENGTH = 2
	- ENCODING_INCLUDED = 4
- UUID = 0x90
- OBJECT = 0xA0
	- EMPTY = OBJECT|0
	- MEMBERS = OBJECT|1
- ARRAY = 0xB0
	- NIL = ARRAY|0
	- ELEMENTS = ARRAY|1
- POINTER = 0xC0
- TIMESTAMP = 0xD0
- DATETIME = 0xE0

Internal types can be divided into *composite* or *basic* depending on whether they are built out of other types or not.  The composite types are OBJECT and ARRAY.  All other types are basic, except for POINTER which is in a class by itself.  Some basic types are *simple* in the sense that they only require their type and no other value data to be stored.  The simple types are NULL, BOOLEAN (TRUE/FALSE), single character strings (SSTRING of one character), EMPTY, and NIL.


Special Values
--------------

- NUL = 0x00 (null pointer usually signifying end of a chain)
- USED = 0x00 (signifying that a pair or array element is in use, meaning not deleted)
- UNUSED = 0x01 (signifying that a pair or array element is unused, meaning deleted)


Header
------

The header of a database file has the layout:

Field        | Contents         | Default | Description
-----        | --------         | ------- | -----------
identifier   | `0x07 BittyDB`   |         | 8 byte (64 bit) "magic number" equal to 523096455619626050 when viewed as a big-endian number, identifying the file type
version      | *n* *version*    |         | format version: *n* is the number of bytes in the *version* number string
charset      | *n* *charset*    | `UTF-8` | character set for strings: *n* is the number of bytes in the *charset* name string
bwidth       | *n*              | `0x05`  | *n* is the width of "big" numbers in the database which are used for file pointers and chunk sizes
cwidth       | *n*              | `0x08`  | *n* is the width of cells in the database which are the basic data "containers"
uuidOption   | `0x10/0x11`      | `0x10`  | option as to whether "_id" fields should be added automatically during insertion (off by default)
buckets      |                  |         | reclaimed storage bucket pointers
root         | `0xA0/0xA1`      | `0xA0`  | root object identifier


Values
------

Generally, all values of any (non-simple) type that can be put into the database, including object keys, have the layout:

Field        | Contents         | Default | Description
-----        | --------         | ------- | -----------
type         | *t*              |         | *t* is the one byte type identifier
data         | *v*              |         | *v* is the value's data

Values of one of the simple types only have the 'type' field.

The header is followed immediately by the *root* object.  All other values in the database can have any type, but the root must be an object.

Objects
-------

An empty object is a simple type with type identifier `0xA0`.

The header of a non-empty object has the layout:

Field        | Contents         | Default | Description
-----        | --------         | ------- | -----------
type         | `0xA1`           |         | identifies a non-empty object
last pointer | *p*              | `NUL`   | *p* points to the last chunk; if the last chunk if the only one, it follows directly and this pointer is NUL

Each chunk of an object has the layout:

Field        | Contents         | Default | Description
-----        | --------         | ------- | -----------
next pointer | *p*              | `NUL`   | *p* points to the next chunk; if this is the last chunk, *p* is NUL
size         | *s*              |         | *s* is the total size in bytes of all the pairs in this chunk including pairs that are marked as removed
pair         | *u* *k* *v*      |         | *u* is 0x00 if this pair has not been removed, 0x01 otherwise; *k* and *v* are a key/value pair - keys are encoded the same way as values
...          | ...              |         | other pairs if they exist
