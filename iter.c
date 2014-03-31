// Copyright (c) 2014 Kevin Martin

struct queryuserdata {
	sqlite3_stmt *stmt;
	int assockey;
	int intkey;
};

static int queryuserdata_mtgc(lua_State *l) {
	lua_settop(l, 1);	//[u]
	assert(lua_type(l, 1) == LUA_TUSERDATA);

	struct queryuserdata *ud =
	 (struct queryuserdata *)lua_touserdata(l, 1);
	assert(ud != NULL);

	if(ud->stmt != NULL) {
		sqlite3_finalize(ud->stmt);
		ud->stmt = NULL;
	}

	return 0;
}

static int queryuserdata_mtcall(lua_State *l) {
	lua_settop(l, 1);	//[u]
	assert(lua_type(l, 1) == LUA_TUSERDATA);

	struct queryuserdata *ud =
	 (struct queryuserdata *)lua_touserdata(l, 1);
	assert(ud != NULL);

	if(ud->stmt == NULL) {
		lua_pushnil(l);	//[u_]
		return 1;
	}

	int rc = sqlite3_step(ud->stmt);
	if(rc == SQLITE_DONE) {
		sqlite3_finalize(ud->stmt);
		ud->stmt = NULL;
		lua_pushnil(l);	//[u_]
		return 1;
	}

	if(rc != SQLITE_ROW) {
		lua_pushfstring(l, 
		 "Failed to execute statement: %s",
		 sqlite3_errmsg(sqlite3_db_handle(ud->stmt)));
		sqlite3_finalize(ud->stmt);
		ud->stmt = NULL;
		return lua_error(l);
	}

	assert(rc == SQLITE_ROW);
	
	lua_newtable(l);	//[ut]
	int col = sqlite3_column_count(ud->stmt) - 1;
	while(col >= 0) {
		switch(sqlite3_column_type(ud->stmt, col)) {
			case SQLITE_NULL:
				lua_pushnil(l);
				break;

			case SQLITE_INTEGER:
			case SQLITE_FLOAT:
				lua_pushnumber(l, sqlite3_column_double(ud->stmt, col));
				break;

			case SQLITE_TEXT:
				lua_pushlstring(l,
				 (const char *)sqlite3_column_text(ud->stmt, col),
				 sqlite3_column_bytes(ud->stmt, col));
				break;

			case SQLITE_BLOB:
				lua_pushlstring(l,
				 (const char *)sqlite3_column_blob(ud->stmt, col),
				 sqlite3_column_bytes(ud->stmt, col));
				break;

			default:
				assert(0);
				return luaL_error(l, "Logic Error: unknown col type");
		}
		//[ut?]

		lua_pushvalue(l, -1);	//[ut??]
		if(ud->intkey) {
			lua_rawseti(l, -3, col+1);	//[ut?]
		} else {
			lua_pop(l, 1);	//[ut?]
		}

		if(ud->assockey) {
			lua_setfield(l, -2, sqlite3_column_name(ud->stmt, col));	//[ut]
		} else {
			lua_pop(l, 1);	//[ut]
		}

		--col;
	}

	return 1;
}

//sets the db metatable on the userdata, and returns the userdata
static int queryuserdata_setmt(lua_State *l) {
	lua_settop(l, 1);	//[u]
	assert(lua_type(l, 1) == LUA_TUSERDATA);

	assert(lua_getmetatable(l, 1) == 0);

	lua_rawgetp(l, LUA_REGISTRYINDEX, (void *)queryuserdata_setmt);
	if(lua_type(l, -1) == LUA_TNIL) {
		//[u_]
		lua_pop(l, 1);	//[u]

		lua_newtable(l);	//[ut]

		lua_pushcfunction(l, queryuserdata_mtcall);	//[utf]
		lua_setfield(l, -2, "__call");	//[ut]

		lua_pushcfunction(l, queryuserdata_mtgc);	//[utf]
		lua_setfield(l, -2, "__gc");	//[ut]

		lua_pushvalue(l, -1);	//[utt]
		lua_rawsetp(l, LUA_REGISTRYINDEX, (void *)queryuserdata_setmt);	//[ut]
	}
	assert(lua_type(l, -1) == LUA_TTABLE);

	lua_setmetatable(l, -2);	//[u]
	return 1;
}

static int queryuserdata_create(lua_State *l) {
	//the dbud, queryobj, params, assoc
	lua_settop(l, 4);	//[uttb]
	assert(lua_type(l, 1) == LUA_TUSERDATA);
	luaL_checktype(l, 2, LUA_TTABLE);
	luaL_checktype(l, 3, LUA_TTABLE);
	luaL_checktype(l, 4, LUA_TBOOLEAN);

	struct dbuserdata *uddb =
	 (struct dbuserdata *)lua_touserdata(l, 1);
	assert(uddb != NULL);
	assert(uddb->db != NULL);

	struct queryuserdata *ud =	//[uttbu]
	 (struct queryuserdata *)lua_newuserdata(l, sizeof(struct queryuserdata));
	assert(ud != NULL);
	ud->stmt = NULL;

	if(lua_toboolean(l, 4)) {
		ud->assockey = 1;
		ud->intkey = 0;
	} else {
		ud->assockey = 0;
		ud->intkey = 1;
	}
	lua_remove(l, 4);	//[uttu]

	lua_pushcfunction(l, queryuserdata_setmt);	//[uttuf]
	lua_insert(l, 4);	//[uttfu]
	lua_call(l, 1, 1);	//[uttu]

	lua_getfield(l, 2, "_fullquery");	//[uttus]

	int rc = sqlite3_prepare_v2(uddb->db, lua_tostring(l, 5),
	 -1, &ud->stmt, NULL);
	if(rc != SQLITE_OK) {
		return luaL_error(l, "Failed to parse SQL '%s': %s",
		 lua_tostring(l, 5), sqlite3_errmsg(uddb->db));
	}
	lua_pop(l, 1);	//[uttu]

	lua_pushvalue(l, 2);	//[uttut]
	lua_setuservalue(l, -2);	//[uttu]
	lua_remove(l, 2);	//[utu];

	//the db is held by the query (the user value)
	lua_remove(l, 1);	//[tu]
	lua_insert(l, 1);	//[ut]

	//Bind the parameters
	lua_getfield(l, -1, "n");	//[utn]
	assert(lua_type(l, -1) == LUA_TNUMBER);
	int n = lua_tointeger(l, -1);	
	lua_pop(l, 1);	//[ut]

	while(n > 0) {
		lua_rawgeti(l, -1, n);	//[ut?]
		switch(lua_type(l, -1)) {
			case LUA_TNIL:
				sqlite3_bind_null(ud->stmt, n);
				break;

			case LUA_TSTRING:
				sqlite3_bind_text(ud->stmt, n, lua_tostring(l, -1),
				 -1, SQLITE_TRANSIENT);
				break;

			case LUA_TNUMBER:
				sqlite3_bind_double(ud->stmt, n, lua_tonumber(l, -1));
				break;

			default:
				return luaL_error(l, "Unable to bind parameter type");
		}
		--n;

		lua_pop(l, 1);	//[ut]
	}

	lua_pop(l, 1);	//[u]
	return 1;
}

