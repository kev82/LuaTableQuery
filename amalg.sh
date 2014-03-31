#!/bin/sh
echo "/*"
echo "   Amalgamation generated on `date`"
echo ""
cat License.txt
echo "*/"
echo "static char ltq_code[] = {"
cat lq.lua | luac -o - - | xxd -i
echo "};"
cat includes.c
cat sud.c rbvt.c sfunc.c iter.c db.c
cat luaopen.c
