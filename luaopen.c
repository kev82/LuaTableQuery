// Copyright (c) 2014 Kevin Martin

int luaopen_ltq(lua_State *l) {
	lua_settop(l, 0);	//[]

	int rc = luaL_loadbufferx(l, ltq_code, sizeof(ltq_code),
	 "ltq_code", "b");	//[f]
	if(rc != LUA_OK) {
		return lua_error(l);
	}
	lua_pushcfunction(l, createdbuserdata);	//[ff]
	lua_call(l, 1, 1);	//[f]
	assert(lua_type(l, -1) == LUA_TTABLE);
	
	return 1;
}
