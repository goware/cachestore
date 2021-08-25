package redis

type Config struct {
	Host string `toml:"host"`
	Port uint16 `toml:"port"`
}
