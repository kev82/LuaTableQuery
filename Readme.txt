Lua Table Query
===============

A Linq-like module for Lua that allows querying Lua tables with the SQLite3
database engine.

Building
========

This is designed/tested on Debian Wheezy with Lua 5.2. It may or may not
work with different Lua versions and operating systems.

1) Modify includes.c appropriately for your system
2) Run amalg.sh > ltq.c

Simply include ltq.c in your own project. The entrypoint is luaopen_ltq.
Symbols for SQLite3 and Lua must be linked in at some point.

To build as a Lua module run

gcc -shared -fPIC -O2 -o ltq.so ltq.c -lsqlite3

Documentation
=============

None yet!

sqtri.lua in the examples demonstrates the use of parameters, datasources,
and scalar functions, which is just about everything currently suported.

Todo
====

This is pretty much at the level that we need it for, so is unlikely
to change soon, but with enough interest we're looking towards
(in rough order of importance).

1) Implementing equality constraints for unique columns. db:newRowTable
	will make an index for specified columns which we will use to
	respond to xBestIndex, making equality joins much faster

2) Ensuring Lua won't move strings so we can use SQLITE_STATIC rather
	than SQLITE_TRANSIENT

3) Passing general Lua values through SQLite, so that they can be returned
	by the result iterators

4) Support for row based iterators, as opposed to row based tables.
	eg db:newRowIterator(function() return ipairs(t))

We are specificaly _not_ intending to support delete/update/insert
