// Copyright (c) 2014 Kevin Martin

struct sfuserdata {
	lua_State *funcState;
	int funcref;
};

static void sfuserdata_caller(sqlite3_context *ctx,
 int argc, sqlite3_value **argv) {
	//Get the userdata
	struct sfuserdata *ud =
	 (struct sfuserdata *)sqlite3_user_data(ctx);

	//User the func thread to make a new thread and clear the
	//func thread
	assert(lua_gettop(ud->funcState) == 0);
	lua_State *l = lua_newthread(ud->funcState);	//[T]
	int lr = luaL_ref(ud->funcState, LUA_REGISTRYINDEX);	//[]

	//Call the function in the new thread
	lua_rawgeti(l, LUA_REGISTRYINDEX, ud->funcref);
	int arg;
	for(arg=0;arg<argc;++arg) {
		switch(sqlite3_value_type(argv[arg])) {
			case SQLITE_TEXT:
				lua_pushlstring(l,
				 (const char *)sqlite3_value_text(argv[arg]),
				 sqlite3_value_bytes(argv[arg]));
				break;

			case SQLITE_INTEGER:
			case SQLITE_FLOAT:
				lua_pushnumber(l, sqlite3_value_double(argv[arg]));
				break;

			case SQLITE_NULL:
				lua_pushnil(l);

			default:
				luaL_unref(ud->funcState, LUA_REGISTRYINDEX, lr);
				sqlite3_result_error(ctx, "func received unknown type", -1);
				return;
		}
	}

	int rc = lua_pcall(l, argc, 1, 0);
	if(rc != LUA_OK) {
		assert(lua_type(l, -1) == LUA_TSTRING);
		sqlite3_result_error(ctx, lua_tostring(l, -1), -1);
		luaL_unref(ud->funcState, LUA_REGISTRYINDEX, lr);
		return;
	}

	//set the appropriate return value
	switch(lua_type(l, 1)) {
		case LUA_TSTRING:
		{
			size_t bytes;
			const char *s = lua_tolstring(l, 1, &bytes);
			sqlite3_result_text(ctx, s, bytes, SQLITE_TRANSIENT);
			break;
		}

		case LUA_TNUMBER:
			sqlite3_result_double(ctx, lua_tonumber(l, 1));
			break;

		case LUA_TNIL:
			sqlite3_result_null(ctx);
			break;

		default:
			sqlite3_result_error(ctx, "func returned disallowed type", -1);
			break;
	}

	//use the func thread to destroy the new thread
	//clear the func thread
	luaL_unref(ud->funcState, LUA_REGISTRYINDEX, lr);
}

static int sfuserdata_mtgc(lua_State *l) {
	lua_settop(l, 1);	//[u]
	assert(lua_type(l, -1) == LUA_TUSERDATA);

	//clear the function
	lua_getuservalue(l, 1);	//[ut]
	lua_getfield(l, -1, "dbud");	//[utu]

	struct dbuserdata *uddb =
	 (struct dbuserdata *)lua_touserdata(l, 3);
	assert(uddb != NULL);
	assert(uddb->db != NULL);
	lua_pop(l, 1);	//[ut]

	lua_getfield(l, -1, "name");	//[uts]
	sqlite3_create_function(uddb->db, lua_tostring(l, -1),
	 -1, SQLITE_ANY, NULL, NULL, NULL, NULL);

	struct sfuserdata *ud =
	 (struct sfuserdata *)lua_touserdata(l, 1);
	if(ud->funcref != -1) {
		luaL_unref(l, LUA_REGISTRYINDEX, ud->funcref);
	}

	return 0;
}

static int sfuserdata_mtlqtype(lua_State *l) {
	lua_pushstring(l, "function");
	return 1;
}

static int sfuserdata_mtgensql(lua_State *l) {
	lua_settop(l, 1);
	assert(lua_type(l, -1) == LUA_TUSERDATA);

	lua_getuservalue(l, -1);
	assert(lua_type(l, -1) == LUA_TTABLE);
	lua_getfield(l, -1, "name");
	assert(lua_type(l, -1) == LUA_TSTRING);
	
	return 1;
}

static int sfuserdata_setmt(lua_State *l) {
	lua_settop(l, 1);	//[u]
	assert(lua_type(l, -1) == LUA_TUSERDATA);

	assert(lua_getmetatable(l, 1) == 0);

	lua_rawgetp(l, LUA_REGISTRYINDEX, (void *)sfuserdata_setmt);
	if(lua_type(l, -1) == LUA_TNIL) {
		//[u_]
		lua_pop(l, 1);	//[u]

		lua_newtable(l);	//[ut]

		lua_pushvalue(l, -1);	//[utt]
		lua_setfield(l, -2, "__index");	//[ut]

		lua_pushcfunction(l, sfuserdata_mtlqtype);	//[utf]
		lua_setfield(l, -2, "lqtype");	//[ut]

		lua_pushcfunction(l, sfuserdata_mtgensql);	//[utf]
		lua_setfield(l, -2, "_gensql");	//[ut]

		lua_pushcfunction(l, sfuserdata_mtgc);	//[utf]
		lua_setfield(l, -2, "__gc");	//[ut]

		lua_pushvalue(l, -1);	//[utt]
		lua_rawsetp(l, LUA_REGISTRYINDEX, (void *)sfuserdata_setmt);	//[ut]
	}
	assert(lua_type(l, -1) == LUA_TTABLE);

	lua_setmetatable(l, -2);	//[u]
	return 1;
}

static int sfuserdata_create(lua_State *l) {
	lua_settop(l, 3);	//[usf]
	assert(lua_type(l, 1) == LUA_TUSERDATA);
	luaL_checktype(l, 2, LUA_TSTRING);
	luaL_checktype(l, 3, LUA_TFUNCTION);

	struct sfuserdata *ud =	//[usfu]
	 (struct sfuserdata *)lua_newuserdata(l, sizeof(struct sfuserdata));
	assert(ud != NULL);
	ud->funcState = NULL;
	ud->funcref = -1;

	lua_pushcfunction(l, sfuserdata_setmt);	//[usfuf]
	lua_insert(l, 4);	//[usffu]
	lua_call(l, 1, 1);	//[usfu]

	lua_newtable(l);	//[usfut]
	lua_pushvalue(l, 2);	//[usfuts]
	lua_setfield(l, -2, "name");	//[usfut]
	lua_pushvalue(l, 1);	//[usfutu]
	lua_setfield(l, -2, "dbud");	//[usfut]
	ud->funcState = lua_newthread(l);	//[usfutT]
	lua_setfield(l, -2, "thread");	//[usfut]
	lua_setuservalue(l, -2);	//[usfu]

	lua_pushvalue(l, 3);	//[usfuf]
	ud->funcref = luaL_ref(l, LUA_REGISTRYINDEX);	//[usfu]

	struct dbuserdata *uddb =
	 (struct dbuserdata *)lua_touserdata(l, 1);
	assert(uddb != NULL);
	assert(uddb->db != NULL);

	sqlite3_create_function(uddb->db, lua_tostring(l, 2),
	 -1, SQLITE_ANY, ud, sfuserdata_caller, NULL, NULL);

	return 1;
}

