-- Copyright (c) 2014 Kevin Martin
local db = require("ltq").newDB()

local square
do
	local t = {}
	for k=1,10000 do t[k] = {n=k, val = k*k} end
	square = db:newRowTable(t)
end

local triangle
do
	local t = {}
	for k=1,10000 do t[k] = {n=k, val = k*(k+1)/2} end
	triangle = db:newRowTable(t)
end

local psquare = db:newScalarFunction(function(n)
	if n > 70 then return "Too Big!" end

	local t = {}
	for k=1,n do
		t[#t+1] = string.rep("s", n)
	end
	return table.concat(t, "\n")
end)

local ptri = db:newScalarFunction(function(n)
	if n > 70 then return "Too Big!" end

	local t = {}
	for k=1,n do
		t[#t+1] = string.rep("t", k)
	end
	return table.concat(t, "\n")
end)


local sqtri = db:newQuery(
 [[
	select
		s.val as val,
		{{fn:psq}}(s.n) as square,
		{{fn:ptr}}(t.n) as triangle
	from 
		{{ds:sq}} as s join
		 {{ds:tri}} as t using(val)]],
 {sq=square, tri=triangle, psq = psquare, ptr = ptri})

local sqtrilimit = db:newQuery(
 [[
	select
		*
	from
		{{ds:sqtri}}
	limit
		{{pa:num}}
		 offset {{pa:start}}]],
 {sqtri=sqtri})

--print the first 3
for row in sqtrilimit:assocIterator({num=3, start=0}) do
	print(string.format("Val: %d\nSquare:\n%s\nTriangle:\n%s\n",
	 row.val, row.square, row.triangle))
end

--print the 5th
for row in sqtrilimit:assocIterator({num=1, start=4}) do
	print(string.format("Val: %d\nSquare:\n%s\nTriangle:\n%s\n",
	 row.val, row.square, row.triangle))
end

local sqtricount = db:newQuery(
 [[
	select
		count(*) as num
	from 
		{{ds:sqtri}} as st
	where
		st.val between {{pa:min}} and {{pa:max}}]],
 {sqtri=sqtri})

print(string.format(
 [[There are %d sqtri(s) with between 25 and 2500 dots]],
 sqtricount:singleResult({min=25, max=2500})))

