// Copyright (c) 2014 Kevin Martin

static int dbuserdata_mtgc(lua_State *l) {
	lua_settop(l, 1);	//[u]
	assert(lua_type(l, -1) == LUA_TUSERDATA);

	struct dbuserdata *ud =
	 (struct dbuserdata *)lua_touserdata(l, -1);
	assert(ud != NULL);

	if(ud->db != NULL) {
		sqlite3_close(ud->db);
		ud->db = NULL;
	}

	return 0;
}

static int dbuserdata_mtcheckquery(lua_State *l) {
	lua_settop(l, 2);	//[us]
	assert(lua_type(l, 1) == LUA_TUSERDATA);
	luaL_checktype(l, 2, LUA_TSTRING);

	struct dbuserdata *ud =
	 (struct dbuserdata *)lua_touserdata(l, 1);

	sqlite3_stmt *stmt = NULL;
	int rc = sqlite3_prepare_v2(ud->db, lua_tostring(l, 2),
	 -1, &stmt, NULL);
	if(rc != SQLITE_OK) {
		lua_pushboolean(l, 0);	//[usb]
		lua_pushstring(l, sqlite3_errmsg(ud->db));
		return 2;
	}
	assert(stmt != NULL);
	sqlite3_finalize(stmt);

	lua_pushboolean(l, 1);	//[usb]
	return 1;
}

static int dbuserdata_mtresultiter(lua_State *l) {
	lua_settop(l, 4);
	lua_pushcfunction(l, queryuserdata_create);
	lua_insert(l, 1);
	lua_call(l, 4, 1);
	return 1;
}

static int dbuserdata_mtcreaterowvt(lua_State *l) {
	lua_pushcfunction(l, luarowvt_createtable);
	lua_insert(l, 1);
	lua_call(l, lua_gettop(l)-1, 1);
	return 1;
}

static int dbuserdata_mtdroprowvt(lua_State *l) {
	lua_pushcfunction(l, luarowvt_droptable);
	lua_insert(l, 1);
	lua_call(l, lua_gettop(l)-1, 1);
	return 1;
}

static int dbuserdata_mtnewscalarfunc(lua_State *l) {
	//dbud, name, func
	lua_pushcfunction(l, sfuserdata_create);
	lua_insert(l, 1);
	lua_call(l, lua_gettop(l)-1, 1);
	assert(lua_type(l, 1) == LUA_TUSERDATA);
	return 1;
}

//sets the db metatable on the userdata, and returns the userdata
static int dbuserdata_setmt(lua_State *l) {
	lua_settop(l, 1);	//[u]
	assert(lua_type(l, -1) == LUA_TUSERDATA);

	assert(lua_getmetatable(l, 1) == 0);

	lua_rawgetp(l, LUA_REGISTRYINDEX, (void *)dbuserdata_setmt);
	if(lua_type(l, -1) == LUA_TNIL) {
		//[u_]
		lua_pop(l, 1);	//[u]

		lua_newtable(l);	//[ut]

		lua_pushvalue(l, -1);	//[utt]
		lua_setfield(l, -2, "__index");	//[ut]

		lua_pushcfunction(l, dbuserdata_mtcreaterowvt);	//[utf]
		lua_setfield(l, -2, "createRowBasedVT");	//[ut]

		lua_pushcfunction(l, dbuserdata_mtdroprowvt);	//[utf]
		lua_setfield(l, -2, "destroyRowBasedVT");	//[ut]

		lua_pushcfunction(l, dbuserdata_mtcheckquery);	//[utf]
		lua_setfield(l, -2, "checkQuery");	//[ut]

		lua_pushcfunction(l, dbuserdata_mtresultiter);	//[utf]
		lua_setfield(l, -2, "resultIterator");	//[ut]

		lua_pushcfunction(l, dbuserdata_mtnewscalarfunc);	//[utf]
		lua_setfield(l, -2, "newScalarFunction");	//[ut]

		lua_pushcfunction(l, dbuserdata_mtgc);	//[utf]
		lua_setfield(l, -2, "__gc");	//[ut]

		lua_pushvalue(l, -1);	//[utt]
		lua_rawsetp(l, LUA_REGISTRYINDEX, (void *)dbuserdata_setmt);	//[ut]
	}
	assert(lua_type(l, -1) == LUA_TTABLE);

	lua_setmetatable(l, -2);	//[u]
	return 1;
}

static int dbuserdata_setuv(lua_State *l) {
	lua_settop(l, 1);	//[u]
	assert(lua_type(l, -1) == LUA_TUSERDATA);

	lua_getuservalue(l, 1);	//[u_]
	assert(lua_type(l, -1) == LUA_TNIL);
	lua_pop(l, 1);	//[u]

	lua_newtable(l);	//[ut]

	lua_newtable(l);	//[utt]
	lua_setfield(l, -2, "threads");	//[ut]

	lua_setuservalue(l, 1);	//[u]
	return 1;
}

static int dbuserdata_registermodule(lua_State *l) {
	lua_settop(l, 3);	//[usp]
	assert(lua_type(l, 1) == LUA_TUSERDATA);
	assert(lua_type(l, 2) == LUA_TSTRING);
	assert(lua_type(l, 3) == LUA_TLIGHTUSERDATA);

	struct dbuserdata *ud =
	 (struct dbuserdata *)lua_touserdata(l, 1);
	assert(ud != NULL);
	assert(ud->db != NULL);

	lua_getuservalue(l, 1);	//[uspt]
	assert(lua_type(l, -1) == LUA_TTABLE);
	lua_getfield(l, -1, "threads");	//[usptt]
	assert(lua_type(l, -1) == LUA_TTABLE);
	lua_State *vtThread = lua_newthread(l);	//[uspttT]
	//I don't care what the ref is, I just want to keep the thread until
	//after sqlite3_close has been called
	luaL_ref(l, -2);	//[usptt] 
	lua_pop(l, 2);	//[usp]

	int rc = sqlite3_create_module(ud->db, lua_tostring(l, 2),
	 (sqlite3_module *)lua_touserdata(l, 3), vtThread);

	if(rc != SQLITE_OK) {
		return luaL_error(l, "Failed to create module %s",
		 lua_tostring(l, 2));
	}

	lua_pop(l, 2);	//[u]
	return 1;
}

static int createdbuserdata(lua_State *l) {
	lua_settop(l, 0);	//[]
	
	struct dbuserdata *ud =	//[u]
	 (struct dbuserdata *)lua_newuserdata(l, sizeof(struct dbuserdata));
	assert(ud != NULL);
	ud->db = NULL;

	lua_pushcfunction(l, dbuserdata_setuv);	//[uf]
	lua_insert(l, 1);	//[fu]
	lua_call(l, 1, 1);	//[u]

	lua_pushcfunction(l, dbuserdata_setmt);	//[uf]
	lua_insert(l, 1);	//[fu]
	lua_call(l, 1, 1);	//[u]

	int rc = sqlite3_open(":memory:", &ud->db);
	if(rc != SQLITE_OK) {
		return luaL_error(l, "Unable to open DB");
	}
	assert(ud->db != NULL);

	lua_pushcfunction(l, dbuserdata_registermodule);	//[uf]
	lua_insert(l, 1);	//[fu]
	lua_pushstring(l, "luarowvt");	//[fus]
	lua_pushlightuserdata(l, &luarowvt_module);	//[fusp]
	lua_call(l, 3, 1);	//[u]

	return 1;
}
