package redis

import "strings"

type CommandClass string

const (
	CommandClassRead     CommandClass = "read"
	CommandClassMutation CommandClass = "mutation"
)

var allowedCommands = map[string]CommandClass{
	"GET":           CommandClassRead,
	"MGET":          CommandClassRead,
	"HGET":          CommandClassRead,
	"HMGET":         CommandClassRead,
	"HGETALL":       CommandClassRead,
	"HKEYS":         CommandClassRead,
	"HVALS":         CommandClassRead,
	"HLEN":          CommandClassRead,
	"LRANGE":        CommandClassRead,
	"LINDEX":        CommandClassRead,
	"LLEN":          CommandClassRead,
	"SMEMBERS":      CommandClassRead,
	"SISMEMBER":     CommandClassRead,
	"SCARD":         CommandClassRead,
	"ZRANGE":        CommandClassRead,
	"ZRANGEBYSCORE": CommandClassRead,
	"ZSCORE":        CommandClassRead,
	"ZCARD":         CommandClassRead,
	"TYPE":          CommandClassRead,
	"TTL":           CommandClassRead,
	"PTTL":          CommandClassRead,
	"EXISTS":        CommandClassRead,
	"KEYS":          CommandClassRead,
	"SCAN":          CommandClassRead,
	"DBSIZE":        CommandClassRead,
	"INFO":          CommandClassRead,
	"COMMAND":       CommandClassRead,
	"SET":           CommandClassMutation,
	"DEL":           CommandClassMutation,
	"EXPIRE":        CommandClassMutation,
	"PERSIST":       CommandClassMutation,
	"RENAME":        CommandClassMutation,
	"LPUSH":         CommandClassMutation,
	"RPUSH":         CommandClassMutation,
	"HSET":          CommandClassMutation,
	"SADD":          CommandClassMutation,
	"ZADD":          CommandClassMutation,
}

func CommandAllowed(command string) (CommandClass, bool) {
	name := strings.ToUpper(strings.TrimSpace(command))
	class, ok := allowedCommands[name]
	return class, ok
}
