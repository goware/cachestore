package redis

type RedisConfig struct {
	Host string `toml:"host"`
	Port uint16 `toml:"port"`
}
