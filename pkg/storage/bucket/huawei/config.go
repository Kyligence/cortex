package huawei

import "flag"

type Config struct {
	AccessKey string `yaml:"access_key"`
	SecretKey string `yaml:"secret_key"`
	Endpoint  string `yaml:"endpoint"`
	Buckets   string `yaml:"buckets"`
}

func (cfg *Config) RegisterFlags(f *flag.FlagSet) {
	cfg.RegisterFlagsWithPrefix("", f)
}

func (cfg *Config) RegisterFlagsWithPrefix(prefix string, f *flag.FlagSet) {
	f.StringVar(&cfg.AccessKey, prefix+"obs.access-key", "", "Huawei Access Key")
	f.StringVar(&cfg.SecretKey, prefix+"obs.secret-key", "", "Huawei Secret Key")
	f.StringVar(&cfg.Endpoint, prefix+"obs.endpoint", "", "Huawei Endpoint")
	f.StringVar(&cfg.Buckets, prefix+"obs.buckets", "", "Huawei OBS Buckets")
}
