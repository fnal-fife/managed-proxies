package main

import (
	"bytes"
	"errors"
	"flag"
	"fmt"
	"io/ioutil"
	"os"
	"os/exec"
	"os/user"
	"path"
	"strings"
	"sync"
	"time"

	"gopkg.in/yaml.v2"
)

//Mutex locks all over the place
//Logging
// Error handling

const (
	globalTimeout uint   = 15                           // Global timeout in seconds
	exptTimeout   uint   = 10                           // Experiment timeout in seconds
	configFile    string = "proxy_push_config_test.yml" // CHANGE ME BEFORE PRODUCTION
)

type flagHolder struct {
	experiment string
	config     string
	test       bool
}

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

type pingNodeStatus struct {
	node    string
	success bool
}

type vomsProxyStatus struct {
	filename string
	success  bool
}

type sshNodeStatus struct {
	node    string
	account string
	role    string
	success bool
}

type copyProxiesStatus struct {
	node    string
	account string
	role    string
	success bool
	err     error
}

func parseFlags() flagHolder {
	var e = flag.String("e", "", "Name of single experiment to push proxies")
	var c = flag.String("c", configFile, "Specify alternate config file")
	var t = flag.Bool("t", false, "Test mode")

	flag.Parse()

	fh := flagHolder{*e, *c, *t}
	return fh
}

func checkUser(authuser string) error {
	cuser, err := user.Current()
	if err != nil {
		return errors.New("Could not lookup current user.  Exiting")
	}
	if cuser.Username != authuser {
		return fmt.Errorf("This must be run as %s.  Trying to run as %s", authuser, cuser.Username)
	}
	return nil
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
		msg := fmt.Sprintf("Initializing a kerb ticket failed.  The error was %s\n", cmdErr)
		fmt.Println(msg)
	}
	// fmt.Println(os.Getenv("KRB5CCNAME"))
	return
}

func pingAllNodes(nodes []string) <-chan pingNodeStatus {
	c := make(chan pingNodeStatus, len(nodes))
	for _, node := range nodes {
		go func(node string) {
			p := pingNodeStatus{node, false}
			pingargs := []string{"-W", "5", "-c", "1", node}
			cmd := exec.Command("ping", pingargs...)
			cmdErr := cmd.Run()
			if cmdErr != nil {
				fmt.Println(cmdErr)
			} else {
				p.success = true
			}
			c <- p
		}(node)
	}
	return c
}

func getProxies(e string, exptConfig ConfigExperiment, globalConfig map[string]string) <-chan vomsProxyStatus {
	c := make(chan vomsProxyStatus)
	var vomsprefix, certfile, keyfile string

	if exptConfig.Vomsgroup != "" {
		vomsprefix = exptConfig.Vomsgroup
	} else {
		vomsprefix = "fermilab:/fermilab/" + e + "/"
	}

	for role, account := range exptConfig.Roles {
		go func(role, account string) {

			vomsstring := vomsprefix + "Role=" + role

			if exptConfig.Certfile != "" {
				certfile = exptConfig.Certfile
			} else {
				certfile = path.Join(globalConfig["CERT_BASE_DIR"], account+".cert")
			}

			if exptConfig.Keyfile != "" {
				keyfile = exptConfig.Keyfile
			} else {
				keyfile = path.Join(globalConfig["CERT_BASE_DIR"], account+".key")
			}

			outfile := account + "." + role + ".proxy"
			outfilePath := path.Join("proxies", outfile)

			vpi := vomsProxyStatus{outfile, false}
			vpiargs := []string{"-rfc", "-valid", "24:00", "-voms",
				vomsstring, "-cert", certfile,
				"-key", keyfile, "-out", outfilePath}

			cmd := exec.Command("/usr/bin/voms-proxy-init", vpiargs...)
			cmdErr := cmd.Run()
			if cmdErr != nil {
				fmt.Println(cmdErr)
			} else {
				fmt.Println("Generated voms proxy: ", outfilePath)
				vpi.success = true
			}
			// if e == "darkside" {
			// 	time.Sleep(time.Duration(10) * time.Second)
			// }

			c <- vpi
		}(role, account)
	}
	return c
}

func testSSHallNodes(nodes []string, roles map[string]string) <-chan sshNodeStatus {
	numreceives := len(nodes) * len(roles)
	c := make(chan sshNodeStatus, numreceives)
	for _, node := range nodes {
		for _, acct := range roles { // Note that eventually, we want to include the key, which is the role
			go func(node, acct string) {
				s := sshNodeStatus{node, acct, "Default Role - CHANGE", false}

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
				} else {
					fmt.Printf("%s\n", cmdOut)
					s.success = true
				}
				c <- s
			}(node, acct)
		}
	}
	return c
}

func copyProxies(e string, exptConfig ConfigExperiment) <-chan copyProxiesStatus {
	c := make(chan copyProxiesStatus)
	// One copy per node and role
	for role, acct := range exptConfig.Roles {
		go func(role, acct string) {
			proxyFile := acct + "." + role + ".proxy"
			proxyFilePath := path.Join("proxies", proxyFile)

			for _, node := range exptConfig.Nodes {
				go func(role, acct, node string) {
					cps := copyProxiesStatus{node, acct, role, false, nil}
					accountNode := acct + "@" + node + ".fnal.gov"
					newProxyPath := path.Join(exptConfig.Dir, acct, proxyFile+".new")
					finalProxyPath := path.Join(exptConfig.Dir, acct, proxyFile)

					sshopts := []string{"-o", "ConnectTimeout=30",
						"-o", "ServerAliveInterval=30",
						"-o", "ServerAliveCountMax=1"}

					scpargs := append(sshopts, proxyFilePath, accountNode+":"+newProxyPath)
					sshargs := append(sshopts, accountNode, "chmod 400 "+newProxyPath+" ; mv -f "+newProxyPath+" "+finalProxyPath)
					scpCmd := exec.Command("scp", scpargs...)
					sshCmd := exec.Command("ssh", sshargs...)

					_, cmdErr := scpCmd.CombinedOutput()
					if cmdErr != nil {
						msg := fmt.Errorf("Copying proxy %s to node %s failed.  The error was %s\n", proxyFile, node, cmdErr)
						cps.err = msg
						c <- cps
						return
					}

					_, cmdErr = sshCmd.CombinedOutput()
					if cmdErr != nil {
						msg := fmt.Errorf("Error changing permission of proxy %s to mode 400 on %s.  The error was %s\n", proxyFile, node, cmdErr)
						cps.err = msg
						c <- cps
						return
					}
				}(role, acct, node)
			}
		}(role, acct)
	}
	return c
}

func experimentWorker(e string, globalConfig map[string]string, exptConfig ConfigExperiment) <-chan experiment {
	c := make(chan experiment)
	expt := experiment{e, true}
	go func() {
		m := &sync.Mutex{}
		badnodes := make(map[string]struct{})

		for _, node := range exptConfig.Nodes {
			badnodes[node] = struct{}{}
		}

		time.Sleep(2 * time.Second)
		// if e == "darkside" {
		// 	time.Sleep(20 * time.Second)
		// }

		m.Lock()
		krb5ccnameCfg := globalConfig["KRB5CCNAME"]
		m.Unlock()

		getKerbTicket(krb5ccnameCfg)

		m.Lock()
		pingChannel := pingAllNodes(exptConfig.Nodes)
		for _ = range exptConfig.Nodes { // Note that we're iterating over the range of nodes so we make sure
			// that we listen on the channel the right number of times
			select {
			case testnode := <-pingChannel:
				if testnode.success {
					delete(badnodes, testnode.node)
				}
			case <-time.After(time.Duration(5) * time.Second):
			}
		}
		m.Unlock()

		badNodesSlice := make([]string, 0, len(badnodes))
		for node := range badnodes {
			badNodesSlice = append(badNodesSlice, node)
		}

		if len(badNodesSlice) > 0 {
			fmt.Println("Bad nodes are: ", badNodesSlice)
		}

		m.Lock()
		vpiChan := getProxies(e, exptConfig, globalConfig)
		for _ = range exptConfig.Roles {
			select {
			case vpi := <-vpiChan:
				if !vpi.success {
					fmt.Printf("Error obtaining %s.  Please check the cert on fifeutilgpvm01.  Continuing to next proxy.\n", vpi.filename)
					expt.success = false
				}
			case <-time.After(time.Duration(5) * time.Second):
				fmt.Printf("Error obtaining proxy for %s:  timeout.  Check log for details. Continuing to next proxy.\n", expt.name)
				expt.success = false
			}
		}
		m.Unlock()

		m.Lock()
		copyChan := copyProxies(e, exptConfig)
		exptTimeoutChan := time.After(time.Duration(exptTimeout) * time.Second)
		for _ = range exptConfig.Nodes {
			for _ = range exptConfig.Roles { // Note that eventually, we want to include the key, which is the role
				select {
				case pushproxy := <-copyChan:
					if !pushproxy.success {
						fmt.Printf("Error pushing proxy for %s.  The error was %s\n", e, pushproxy.err)
						expt.success = false
					}
				case <-exptTimeoutChan:
					fmt.Printf("Experiment %s hit the timeout when waiting to push proxy.\n", expt.name)
					expt.success = false
				}
				// testSSH(acct, node) // Placeholder for other operations
			}
		}
		// fmt.Printf("%v\n", exptConfig.Nodes)
		m.Unlock()

		//		expt.success = true
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

	// Parse flags
	flags := parseFlags()
	if flags.test {
		fmt.Println("This is in test mode")
	}
	//	fmt.Println(flags)

	// Read the config file
	fmt.Printf("Using config file %s\n", flags.config)
	source, err := ioutil.ReadFile(flags.config)
	if err != nil {
		fmt.Println(err)
		os.Exit(2)
	}
	//fmt.Printf("File contents: %s", source)
	err = yaml.Unmarshal(source, &cfg)
	if err != nil {
		fmt.Println(err)
		os.Exit(2)
	}
	//	fmt.Printf("foo Value: %#v\n, Value: %#v\n", cfg.Logs, cfg.Notifications)
	// fmt.Printf("%v\n", cfg.Experiments["mu2e"].Emails)

	// Check that we're running as the right user
	if err = checkUser(cfg.Global["should_runuser"]); err != nil {
		fmt.Println(err)
		os.Exit(3)
	}

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
			if expt.success {
				experimentSuccess[expt.name] = true
			}
			// fmt.Println(expt.name, expt.success)
		case <-timeout:
			fmt.Println("hit the global timeout!")
			cleanup(experimentSuccess)
			return
		}
	}
	cleanup(experimentSuccess)
}
