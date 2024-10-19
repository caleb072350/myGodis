package db

func MakeRouter() map[string]CmdFunc {
	routerMap := make(map[string]CmdFunc)
	routerMap["ping"] = Ping

	routerMap["del"] = Del
	routerMap["expire"] = Expire
	routerMap["expireat"] = ExpireAt

	routerMap["publish"] = Publish

	return routerMap
}
