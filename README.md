# TinyDB
A tiny one-file document-database system for embedding in other projects

Based on https://github.com/mbdavid/FileDB

## Purpose

This is a very small, path-based data store. It is reduced to as few files as possible, with the intent of being included in other projects as source code, rather than a binary.

If you need something similar but more capable, I recommend https://github.com/mbdavid/LiteDB or https://www.sqlite.org

## Plans

* [ ] Fast file seek based on internal file path
* [ ] Reduce code to minimum
* [ ] Remove requirement to access disk (use only seekable stream) to support UWP.
* [ ] Bring in ( https://github.com/i-e-b/DiskQueue ) as a journal file?

## Using

See the unit tests for now.
