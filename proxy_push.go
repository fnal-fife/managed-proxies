package main

import (
	"bytes"
	"fmt"
	"io/ioutil"
	"os"
	"os/exec"
	"strings"
	"sync"
	"time"

	"gopkg.in/yaml.v2"
)

const (
	globalTimeout int    = 10 // Global timeout in seconds
	configFile    string = "proxy_push_config.yml"
)

type experiment struct {
	name string
	//	nodes []string
	success bool
}

type config struct {
	Logs               map[string]string
	Notifications      map[string]string
	Notifications_test map[string]string
	Global             map[string]string
	Experiments        map[string]ConfigExperiment
}

type ConfigExperiment struct {
	Dir       string
	Emails    []string
	Nodes     []string
	Roles     map[string]string
	Vomsgroup string
	Certfile  string
	Keyfile   string
}

func getKerbTicket(krb5ccname string) {
	os.Setenv("KRB5CCNAME", krb5ccname)
	// fmt.Println(os.Environ())

	kerbcmdargs := []string{"-k", "-t",
		"/opt/gen_keytabs/config/gcso_monitor.keytab",
		"monitor/gcso/fermigrid.fnal.gov@FNAL.GOV"}

	cmd := exec.Command("/usr/krb5/bin/kinit", kerbcmdargs...)
	_, cmdErr := cmd.CombinedOutput()
	if cmdErr != nil {
		msg := "Initializing a kerb ticket failed.  The error was BLAH"
		fmt.Println(msg)
		fmt.Println(cmdErr)
	}
	fmt.Println(os.Getenv("KRB5CCNAME"))
	return
}

func testSSH() {

}

func experimentWorker(e string, globalConfig map[string]string, exptConfig ConfigExperiment) <-chan experiment {
	c := make(chan experiment)
	expt := experiment{e, false}
	go func() {
		m := &sync.Mutex{}
		time.Sleep(2 * time.Second)
		if e == "darkside" {
			time.Sleep(20 * time.Second)
		}

		m.Lock()
		krb5ccnameCfg := globalConfig["KRB5CCNAME"]
		m.Unlock()

		getKerbTicket(krb5ccnameCfg)

		m.Lock()

		for _, node := range exptConfig.Nodes {
			for _, acct := range exptConfig.Roles { // Note that eventually, we want to include the key, which is the role

				var b bytes.Buffer
				b.WriteString(acct)
				b.WriteString("@")
				b.WriteString(node)
				acctHost := b.String()

				sshargs := []string{"-o", "StrictHostKeyChecking=no", acctHost, "hostname"}
				cmd := exec.Command("ssh", sshargs...)
				cmdOut, cmdErr := cmd.CombinedOutput()
				if cmdErr != nil {
					fmt.Println("OOPS!")
					fmt.Println(cmdErr)
				}
				fmt.Println(cmdOut)
			}
		}

		// fmt.Printf("%v\n", exptConfig.Nodes)
		m.Unlock()
		expt.success = true
		c <- expt
		close(c)
	}()
	return c
}

func manageExperimentChannels(exptList []string, cfg config) <-chan experiment {
	agg := make(chan experiment)
	exptChans := make([]<-chan experiment, len(exptList))

	go func() {
		// Start all of the experiment workers
		for _, expt := range exptList {
			exptChans = append(exptChans, experimentWorker(expt, cfg.Global, cfg.Experiments[expt]))
		}

		// Launch goroutines that listen on experiment channels.  Since each experimentWorker closes its channel,
		// each of these goroutines should exit after that happens
		for _, exptChan := range exptChans {
			go func(c <-chan experiment) {
				for expt := range c {
					agg <- expt
				}
			}(exptChan)
		}

	}()

	return agg
}

func cleanup(success map[string]bool) {
	s := make([]string, 0, len(success))
	f := make([]string, 0, len(success))

	for expt := range success {
		if success[expt] {
			s = append(s, expt)
		} else {
			f = append(f, expt)
		}
	}

	fmt.Printf("Successes: %v\n", strings.Join(s, ", "))
	fmt.Printf("Failures: %v\n", strings.Join(f, ", "))

	return
}

func main() {
	var cfg config
	experimentSuccess := make(map[string]bool)

	//  Test
	// cmd := exec.Command("ls", "-l")
	// cmdOut, cmdErr := cmd.CombinedOutput()
	// // err1 := cmd.Run()
	// if cmdErr != nil {
	// 	fmt.Println("uh oh")
	// 	fmt.Println(cmdErr)
	// }
	// fmt.Printf("%s\n", cmdOut)

	// Read the config file
	fmt.Printf("Using config file %s\n", configFile)
	source, err := ioutil.ReadFile(configFile)
	if err != nil {
		panic(err)
	}
	//fmt.Printf("File contents: %s", source)
	err = yaml.Unmarshal(source, &cfg)
	if err != nil {
		panic(err)
	}
	//	fmt.Printf("foo Value: %#v\n, Value: %#v\n", cfg.Logs, cfg.Notifications)
	// fmt.Printf("%v\n", cfg.Experiments["mu2e"].Emails)

	// Check that we're running as the right user
	//CODE

	// Get our list of experiments from the config file
	expts := make([]string, len(cfg.Experiments))
	i := 0
	for k := range cfg.Experiments {
		expts[i] = k
		i++
	}

	// Initialize our successful experiments slice
	for _, expt := range expts {
		experimentSuccess[expt] = false
	}

	// Start up the expt manager
	c := manageExperimentChannels(expts, cfg)
	// Listen on the manager channel
	timeout := time.After(time.Duration(globalTimeout) * time.Second)
	for i := 0; i < len(expts); i++ {
		select {
		case expt := <-c:
			experimentSuccess[expt.name] = true
			// fmt.Println(expt.name, expt.success)
		case <-timeout:
			fmt.Println("hit the global timeout!")
			cleanup(experimentSuccess)
			return
		}
	}
	cleanup(experimentSuccess)
}
