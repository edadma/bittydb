BittyDB File Format
===================

Header
------

Field          | Contents         | Default | Description
-----          | --------         | ------- | -----------
identifier     | `0x07` `BittyDB` |         | 8 byte (64 bit) "magic number" equal to 523096455619626050 when viewed as a big-endian number, serving to identify the file type
version        | <n> <version>    |         | format version: *n* is the number of bytes in the *version* number string
charset        | <n> <charset>    | `UTF-8` | character set for strings: *n* is the number of bytes in the *charset* name string
bwidth         | <n>              | `0x05`  | *n* is the width of "big" numbers in the database which are used for file pointers and chunk sizes