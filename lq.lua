-- Copyright (c) 2014 Kevin Martin
do
	local newdbuserdata = ...
	if type(newdbuserdata) ~= "function" then
		error("lqt loader used incorrectly")
	end

	--this object is used to generate full parameter names,
	--and map them to numbers
	local newParamObj
	do
		local paramobj_mt = {}
		paramobj_mt.__index = paramobj_mt

		function paramobj_mt:push(arg)
			self._stack[#self._stack+1] = arg
		end

		function paramobj_mt:pop()
			self._stack[#self._stack] = nil
		end

		function paramobj_mt:sqlname(p)
			self:push(p)
			p = table.concat(self._stack, "/")
			self:pop()

			local byname = self._paramByName
			local byidx = self._paramByIndex

			if not byname[p] then
				byidx[#byidx+1] = p
				byname[p] = #byidx
			end

			return string.format("?%d", byname[p])
		end

		function paramobj_mt:paramTblIndex()
			return self._paramByIndex
		end

		function paramobj_mt:paramTblString()
			return self._paramByName
		end

		function newParamObj()
			local rv = {}
			rv._stack = {}
			rv._paramByIndex = {}
			rv._paramByName = {}

			setmetatable(rv, paramobj_mt)

			return rv
		end
	end

	--will create a virtual table based on a row style real table
	local newRowBasedTableObject
	do
		local ds_mt = {}
		ds_mt.__index = ds_mt

		function ds_mt:_gensql(subquery)
			return self._name
		end

		function ds_mt:__gc()
			self._dbud:destroyRowBasedVT(self._name, self._regidx)
		end

		function ds_mt:lqtype()
			return "datasource"
		end

		local function getcols(t)
			if #t < 1 then error("Need at least one row") end
			local cols = {}
			for _, row in ipairs(t) do
				if not next(row) then
					error("Empty row")
				end
				for k in pairs(row) do
					cols[k] = true
				end
			end

			local cols2 = {}
			for k in pairs(cols) do
				cols2[#cols2+1] = k
			end

			return cols2 
		end

		function newRowBasedTableObject(dbud, name, tbl)
			local rv = {}
			rv._name = name
			rv._dbud = dbud

			local cols = getcols(tbl)
			rv._regidx = dbud:createRowBasedVT(name, cols, tbl)
			assert(type(rv._regidx) == "number")
			
			setmetatable(rv, ds_mt)
			return rv
		end
	end

	local newQueryObject
	do
		local query_mt = {}
		query_mt.__index = query_mt

		function query_mt:_gensql(subquery, paramobj)
			paramobj = paramobj or newParamObj()

			--replace all the data sources
			local sql = self._basicquery:gsub([[{{ds:([%a%d]+)}}]],
			 function(m)
				local t = self._sources
				if not t[m] then
					error(string.format("Unknown datasource '%s'", m))
				end
				if t[m]:lqtype() ~= "datasource" then
					error(string.format("Not a datasource '%s'", m))
				end
				paramobj:push(m)
				local rv = t[m]:_gensql(true, paramobj)
				paramobj:pop()
				return rv
			end)

			--replace the parameters
			sql = sql:gsub([[{{pa:([%a%d]+)}}]],
			 function(p)
				return paramobj:sqlname(p)
			end)

			--and the functions
			sql = sql:gsub([[{{fn:([%a%d]+)}}]],
			 function(m)
				local t = self._sources
				if not t[m] then
					error(string.format("Unknown function '%s'", m))
				end
				if t[m]:lqtype() ~= "function" then
					error(string.format("Not a function '%s'", m))
				end
				return t[m]:_gensql(true, paramobj)
			end)

			if subquery then sql = string.format("(%s)", sql) end
			return sql, paramobj
		end

		function query_mt:_checkparams(params)
			params = params or {}
			local rv = {}
			rv.n = #(self._params:paramTblIndex())
			local validparam = self._params:paramTblString()
			for p, v in pairs(params) do
				if not validparam[p] then
					error(string.format(
					 "Invalid parameter %s", p))
				end
				rv[validparam[p]] = v
			end
			return rv
		end

		function query_mt:lqtype()
			return "datasource"
		end

		function query_mt:assocIterator(params)
			params = self:_checkparams(params)
			return self._dbud:resultIterator(self, params, true)
		end

		function query_mt:intIterator(params)
			params = self:_checkparams(params)
			return self._dbud:resultIterator(self, params, false)
		end

		function query_mt:singleResult(params)
			params = self:_checkparams(params)
			local rv = self._dbud:resultIterator(self, params, false)
			rv = rv()
			return rv[1]
		end

		function newQueryObject(dbud, q, s)
			local rv = {}
			rv._dbud = dbud
			rv._basicquery = q
			rv._sources = s
			setmetatable(rv, query_mt)

			rv._fullquery, rv._params = rv:_gensql()
			local ok, err = dbud:checkQuery(rv._fullquery)
			if not ok then
				error(string.format(
				 "Failed to parse generated SQL '%s': %s",
				 rv._fullquery, err))
			end
			return rv
		end
	end

	local newdbobj		
	do
		local db_mt = {}
		db_mt.__index = db_mt

		function db_mt:newRowTable(t)
			self._ntables = self._ntables+1
			return newRowBasedTableObject(self._dbud,
			 string.format([["_lq_table%d"]], self._ntables), t)
		end

		function db_mt:newScalarFunction(f)
			self._nfuncs = self._nfuncs+1
			return self._dbud:newScalarFunction(
			 string.format([[_lq_func%d]], self._nfuncs), f)
		end

		function db_mt:newQuery(q, s)
			return newQueryObject(self._dbud, q, s)
		end

		function newdbobj()
			local rv = {}

			rv._ntables = 0
			rv._nfuncs = 0
			rv._dbud = newdbuserdata()

			setmetatable(rv, db_mt)
			return rv
		end
	end

	return {
	 newDB = newdbobj
	}
end
