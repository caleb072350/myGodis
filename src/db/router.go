package db

func MakeRouter() map[string]CmdFunc {
	routerMap := make(map[string]CmdFunc)

	routerMap["del"] = Del
	routerMap["expire"] = Expire
	routerMap["expireat"] = ExpireAt

	routerMap["flushdb"] = FlushDB
	routerMap["flushall"] = FlushAll

	return routerMap
}
