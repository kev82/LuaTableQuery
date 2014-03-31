// Copyright (c) 2014 Kevin Martin

struct luarowvt_vtab {
	struct sqlite3_vtab vtab;
	
	lua_State *modState;
	int regIdx;
};

static int luarowvt_create(sqlite3 *db, void *p,
 int argc, const char *const *argv, sqlite3_vtab **pvtab, char **perr) {
	//We need the vt state (p) in order to create and destroy cursors
	//so we need to store that. We need to store the registry index with
	//our table in it.
	//we also need to declare the vtab based on the cols table

	*pvtab = NULL;
	*perr = NULL;

	if(argc != 4) {
		return SQLITE_ERROR;
	}

	int regidx = -1;
	if(sscanf(argv[3], "%d", &regidx) != 1 || regidx == -1)  {
		return SQLITE_ERROR;
	}

	lua_State *modState = (lua_State *)p; 
	assert(lua_gettop(modState) == 0);
	lua_rawgeti(modState, LUA_REGISTRYINDEX, regidx);	//[?]
	if(lua_type(modState, -1) != LUA_TTABLE) {
		lua_settop(modState, 0);	//[]
		return SQLITE_ERROR;
	}
	lua_settop(modState, 0);	//[]

	struct luarowvt_vtab *v =
	 (struct luarowvt_vtab *)sqlite3_malloc(sizeof(struct luarowvt_vtab));
	assert(v != NULL);
	v->modState = p;
	v->regIdx = regidx;

	char *vtabsql;
	size_t bytes;
	FILE *sqlstream = open_memstream(&vtabsql, &bytes);
	assert(sqlstream != NULL);
	fprintf(sqlstream, "create table t(");
	
	lua_rawgeti(modState, LUA_REGISTRYINDEX, regidx);	//[t]
	lua_getfield(modState, 1, "cols");	//[tt]
	assert(lua_type(modState, -1) == LUA_TTABLE);
	lua_rawgeti(modState, -1, 1);	//[tts]
	assert(lua_type(modState, -1) == LUA_TSTRING);
	fprintf(sqlstream, "%s", lua_tostring(modState, -1));
	lua_pop(modState, 1);	//[tt]

	int i;
	for(i=2;i<=lua_rawlen(modState, -1);++i) {
		lua_rawgeti(modState, -1, i);	//[tts]
		assert(lua_type(modState, -1) == LUA_TSTRING);
		fprintf(sqlstream, ", %s", lua_tostring(modState, -1));
		lua_pop(modState, 1);	//[tt]
	}

	lua_settop(modState, 0);	//[]

	fprintf(sqlstream, ")");
	fclose(sqlstream);

	sqlite3_declare_vtab(db, vtabsql);
	free(vtabsql); 

	*pvtab = (sqlite3_vtab *)v;
	(*pvtab)->zErrMsg = NULL;
	return SQLITE_OK;
}

static int luarowvt_connect(sqlite3 *db, void *p,
 int argc, const char *const *argv, sqlite3_vtab **pvtab, char **perr) {
	*pvtab = NULL;
	*perr = NULL;
	return SQLITE_ERROR;
}

static int luarowvt_disconnect(sqlite3_vtab *vtab) {
	//All we have to do here is unref the table description
	//from the registry
	struct luarowvt_vtab *v = (struct luarowvt_vtab *)vtab;
	assert(lua_gettop(v->modState) == 0);
	luaL_unref(v->modState, LUA_REGISTRYINDEX, v->regIdx);
	
	return SQLITE_OK;
}

static int luarowvt_bestidx(sqlite3_vtab *vtab, sqlite3_index_info *info) {
	return SQLITE_OK;
}

struct luarowvt_cursor {
	struct sqlite3_vtab_cursor cur;

	lua_State *myThread;
};

static int luarowvt_cursopen(sqlite3_vtab *vtab, sqlite3_vtab_cursor **cur) {
	//We have to create a new thread and anchor it in the vt's conf table
	//We then need to push the actual table and starting index onto the
	//new thread

	struct luarowvt_vtab *v = (struct luarowvt_vtab *)vtab;
	assert(lua_gettop(v->modState) == 0);

	struct luarowvt_cursor *c =
	 (struct luarowvt_cursor *)sqlite3_malloc(sizeof(struct luarowvt_cursor));
	assert(c != NULL);

	lua_rawgeti(v->modState, LUA_REGISTRYINDEX, v->regIdx);	//[t]
	assert(lua_type(v->modState, -1) == LUA_TTABLE);
	lua_getfield(v->modState, -1, "threads");	//[tt]
	assert(lua_type(v->modState, -1) == LUA_TTABLE);

	c->myThread = lua_newthread(v->modState);	//[ttT]

	luaL_ref(v->modState, -2);	//[tt]
	lua_pop(v->modState, 2);	//[]

	assert(lua_gettop(v->modState) == 0);

	lua_rawgeti(c->myThread, LUA_REGISTRYINDEX, v->regIdx);	//[t]
	assert(lua_type(c->myThread, -1) == LUA_TTABLE);
	lua_getfield(c->myThread, -1, "data");	//[tt]
	lua_remove(c->myThread, 1);	//[t]
	lua_pushnil(c->myThread);	//[t_]

	*cur = (sqlite3_vtab_cursor *)c;
	return SQLITE_OK;
}

static int luarowvt_cursclose(sqlite3_vtab_cursor *cur) {
	//get the mod state, and table index
	//search the thread table for the thread myThread, and unref it

	struct luarowvt_vtab *v = (struct luarowvt_vtab *)(cur->pVtab);
	assert(lua_gettop(v->modState) == 0);

	struct luarowvt_cursor *c = (struct luarowvt_cursor *)cur;

	lua_rawgeti(v->modState, LUA_REGISTRYINDEX, v->regIdx);	//[t]
	assert(lua_type(v->modState, -1) == LUA_TTABLE);
	lua_getfield(v->modState, 1, "threads");	//[tt]
	assert(lua_type(v->modState, -1) == LUA_TTABLE);

	lua_pushnil(v->modState);	//[tt_]
	while(lua_next(v->modState, 2) != 0) {
		//[ttnT]
		assert(lua_type(v->modState, -2) == LUA_TNUMBER);
		assert(lua_type(v->modState, -1) == LUA_TTHREAD);

		if(lua_tothread(v->modState, -1) == c->myThread) {
			lua_pushvalue(v->modState, -2);	//[ttnTn]
			lua_pushnil(v->modState);	//[ttnTn_]
			lua_settable(v->modState, 2);	//[ttnT]
		}
		lua_pop(v->modState, 1);	//[ttn]
	}
	//[tt]

	lua_pop(v->modState, 2);	//[]

	sqlite3_free(cur);
	return SQLITE_OK;
}

static int luarowvt_cursfilter(sqlite3_vtab_cursor *cur, int idxNum,
 const char *idxStr, int argc, sqlite3_value **argv) {
	struct luarowvt_cursor *c = (struct luarowvt_cursor *)cur;

	lua_settop(c->myThread, 1);
	assert(lua_type(c->myThread, 1) == LUA_TTABLE);
	lua_pushnil(c->myThread);
	lua_next(c->myThread, 1);
	
	return SQLITE_OK;
}

static int luarowvt_curseof(sqlite3_vtab_cursor *cur) {
	struct luarowvt_cursor *c = (struct luarowvt_cursor *)cur;
	return lua_gettop(c->myThread) == 1;
}

static int luarowvt_cursnext(sqlite3_vtab_cursor *cur) {
	struct luarowvt_cursor *c = (struct luarowvt_cursor *)cur;
	if(lua_gettop(c->myThread) != 3) return SQLITE_OK;

	lua_pop(c->myThread, 1);
	lua_next(c->myThread, 1);
	
	return SQLITE_OK;
}

static int luarowvt_lookupcolumnindex(lua_State *l) {
	lua_settop(l, 2);	//[nn]
	int regIdx = luaL_checkint(l, 1);
	int colIdx = luaL_checkint(l, 2);
	assert(colIdx > 0);
	lua_pop(l, 2);	//[]

	lua_rawgeti(l, LUA_REGISTRYINDEX, regIdx);	//[t]
	assert(lua_type(l, -1) == LUA_TTABLE);
	lua_getfield(l, -1, "cols");	//[tt]
	assert(lua_type(l, -1) == LUA_TTABLE);
	lua_rawgeti(l, -1, colIdx);	//[tts]
	assert(lua_type(l, -1) == LUA_TSTRING);

	return 1;
}

static int luarowvt_curscol(sqlite3_vtab_cursor *cur,
 sqlite3_context *ctx, int col) {
	struct luarowvt_cursor *c = (struct luarowvt_cursor *)cur;
	assert(lua_gettop(c->myThread) == 3);	//[tnt]

	struct luarowvt_vtab *v = (struct luarowvt_vtab *)cur->pVtab;

	lua_pushcfunction(c->myThread, luarowvt_lookupcolumnindex);	//[tntf]
	lua_pushinteger(c->myThread, v->regIdx);	//[tntfn]
	lua_pushinteger(c->myThread, col+1);	//[tntfnn]
	int rc = lua_pcall(c->myThread, 2, 1, 0);	//[tnts]
	assert(rc == LUA_OK);
	assert(lua_type(c->myThread, -1) == LUA_TSTRING);
	lua_gettable(c->myThread, -2);	//[tnt?]

	switch(lua_type(c->myThread, -1)) {
		case LUA_TSTRING:
		{
			size_t bytes;
			const char *str = lua_tolstring(c->myThread, -1, &bytes);
			sqlite3_result_text(ctx, str, bytes, SQLITE_TRANSIENT);
			break;
		}

		case LUA_TNUMBER:
			sqlite3_result_double(ctx, lua_tonumber(c->myThread, -1));
			break;

		case LUA_TNIL:
			sqlite3_result_null(ctx);
			break;

		default:
			sqlite3_result_error(ctx, "Bad type in table", -1);
			break;
	}

	lua_pop(c->myThread, 1);	//[tnt]

	return SQLITE_OK;
}

static int luarowvt_cursrid(sqlite3_vtab_cursor *cur,
 sqlite3_int64 *prowid) {
	struct luarowvt_cursor *c = (struct luarowvt_cursor *)cur;
	assert(lua_gettop(c->myThread) == 3);	//[tnt]
	assert(lua_type(c->myThread, 2) == LUA_TNUMBER);
	*prowid = lua_tointeger(c->myThread, 2);
	return SQLITE_OK;
}

static int luarowvt_rename(sqlite3_vtab *vtab, const char *name) {
	return SQLITE_ERROR;
}

static sqlite3_module luarowvt_module = {
 1,
 luarowvt_create,
 luarowvt_connect,
 luarowvt_bestidx,
 luarowvt_disconnect,
 luarowvt_disconnect,
 luarowvt_cursopen,
 luarowvt_cursclose,
 luarowvt_cursfilter,
 luarowvt_cursnext,
 luarowvt_curseof,
 luarowvt_curscol,
 luarowvt_cursrid,
 NULL,
 NULL,
 NULL,
 NULL,
 NULL,
 NULL,
 luarowvt_rename};

static int luarowvt_createtable(lua_State *l) {
	lua_settop(l, 4);	//[ustt]
	assert(lua_type(l, 1) == LUA_TUSERDATA);
	luaL_checktype(l, 2, LUA_TSTRING);
	luaL_checktype(l, 3, LUA_TTABLE);
	luaL_checktype(l, 4, LUA_TTABLE);

	//check the table name is quoted
	{
		size_t len;
		const char *s = lua_tolstring(l, 2, &len);
		if(s[0] != '"' || s[len-1] != '"') {
			return luaL_error(l, "Table name must be quoted");
		}
	}

	lua_newtable(l);	//[usttt]
	lua_insert(l, 3);	//[uttst]
	lua_setfield(l, 3, "data");	//[ustt]
	lua_setfield(l, 3, "cols");	//[ust]
	lua_newtable(l);	//[ustt]
	lua_setfield(l, -2, "threads");	//[ust]
	int ref = luaL_ref(l, LUA_REGISTRYINDEX);	//[us]

	char *query = sqlite3_mprintf(
	 "create virtual table %s using luarowvt (%d)",
	 lua_tostring(l, 2), ref);
	assert(query != NULL);

	struct dbuserdata *ud =
	 (struct dbuserdata *)lua_touserdata(l, 1);
	
	int rc = sqlite3_exec(ud->db, query, NULL, NULL, NULL);
	if(rc != SQLITE_OK) {
		luaL_unref(l, LUA_REGISTRYINDEX, ref);
		return luaL_error(l, "Failed to create vt: %s",
		 sqlite3_errmsg(ud->db));
	}

	lua_pushinteger(l, ref);
	return 1;
}

static int luarowvt_droptable(lua_State *l) {
	lua_settop(l, 3);
	assert(lua_type(l, 1) == LUA_TUSERDATA);	//[usn]
	luaL_checktype(l, 2, LUA_TSTRING);
	luaL_checktype(l, 3, LUA_TNUMBER);

	char *query = sqlite3_mprintf(
	 "drop table %s", lua_tostring(l, 2));
	assert(query != NULL);

	struct dbuserdata *ud =
	 (struct dbuserdata *)lua_touserdata(l, 1);

	int rc = sqlite3_exec(ud->db, query, NULL, NULL, NULL);
	if(rc != SQLITE_OK) {
		return luaL_error(l, "Failed to drop vt: %s",
		 sqlite3_errmsg(ud->db));
	}

	luaL_unref(l, LUA_REGISTRYINDEX, lua_tointeger(l, 3));
	return 0;
}
