package main

import (
	"fmt"

	"github.com/rifflock/lfshook"
	log "github.com/sirupsen/logrus"
	"github.com/spf13/pflag"
	"github.com/spf13/viper"

	"cdcvs.fnal.gov/discompsupp/ken_proxy_push/notifications"
	"cdcvs.fnal.gov/discompsupp/ken_proxy_push/proxy"
)

const configFile string = "managedProxies.yml"

func main() {
	fmt.Println("vim-go")
	/* Order of operations

	1. Ingest service certs
	2. Check expiration dates
	3. If any cert expires in less than 30 days, warn
	4.  Else, tell us when the next expiration is
	*/
}
