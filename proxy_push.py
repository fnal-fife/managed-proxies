#!/usr/bin/env python
import subprocess
import json
import sys
import logging
import requests
from logging.handlers import RotatingFileHandler
from traceback import format_exc
from os import environ, geteuid, remove
from pwd import getpwuid
import smtplib
import email.utils
from email.mime.text import MIMEText


# Global Variables

# JSON input file
inputfile = 'input_file.json'

# Logging/Error handling variables.
logfile = 'proxy_push.log'
errfile = 'proxy_push.err'      # Set the temporary output file for errors.  Times in errorfile are local time.
logger = None

SLACK_ALERTS_URL = 'https://hooks.slack.com/services/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'      # Slack url for #alerts channel in fife-group slack
# SLACK_ALERTS_URL = 'https://hooks.slack.com/services/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'      # Slack url for #alerts-dev channel in fife-group slack.  Use for testing

admin_email = 'fife-group@fnal.gov'
# admin_email = 'sbhat@fnal.gov'
# admin_email = 'kherner@fnal.gov'

# Displays who is running this script.  Will not allow running as root
should_runuser = 'rexbatch'

# # grab the initial environment
KRB5CCNAME = "FILE:/tmp/krb5cc_push_proxy"

# base location for certs/keys
CERT_BASE_DIR = "/opt/gen_push_proxy/certs"

# Experiments that have their own VOMS server
expt_vos = ["des", "dune"]

# Functions


def sendemail():
    """Function to send email after message string is populated."""
    with open(errfile, 'r') as f:
        message = f.read()

    sender = 'fife-group@fnal.gov'
    to = admin_email
    msg = MIMEText(message)
    msg['To'] = email.utils.formataddr(('FIFE GROUP', to))
    msg['From'] = email.utils.formataddr(('FIFEUTILGPVM01', sender))
    msg['Subject'] = "proxy_push.py errors"

    try:
        smtpObj = smtplib.SMTP('smtp.fnal.gov')
        smtpObj.sendmail(sender, to, msg.as_string())
        smsg = "Successfully sent error email"
        if logger is not None:
            logger.info(smsg)
    except Exception as e:
        err = "Error:  unable to send email.\n%s\n" % e
        if logger is not None:
            error_handler(err)
        else:
            print err
        raise


def sendslackmessage():
    """Function to send notification to fife-group #alerts slack channel"""
    with open(errfile, 'r') as f:
        payloadtext = f.read()

    payload = {"text": payloadtext}
    headers = {"Content-type": "application/json"}

    r = requests.post(SLACK_ALERTS_URL, data=json.dumps(payload),
                      headers=headers)

    if r.status_code != requests.codes.ok:
        errmsg = "Could not send slack message.  " \
                 "Status code {0}, response text {1}".format(
                    r.status_code, r.text)
        if logger is not None:
            error_handler(errmsg)
        else:
            print errmsg


def setup_logger(name):
    """Sets up the logger"""
    global logger
    # Create Logger
    logger = logging.getLogger(name)
    logger.setLevel(logging.DEBUG)

    # Console handler - info
    ch = logging.StreamHandler()
    ch.setLevel(logging.INFO)

    # Logfile Handler
    lh = RotatingFileHandler(logfile, maxBytes=2097152, backupCount=3)
    lh.setLevel(logging.DEBUG)
    logfileformat = logging.Formatter(
        "%(asctime)s - %(name)s - %(levelname)s - %(message)s")
    lh.setFormatter(logfileformat)

    # Errorfile Handler
    eh = logging.FileHandler(errfile)
    eh.setLevel(logging.WARNING)
    errfileformat = logging.Formatter(
        "%(asctime)s - %(levelname)s - %(message)s")
    eh.setFormatter(errfileformat)

    for handler in [ch, lh, eh]:
        logger.addHandler(handler)

    return logger


def error_handler(exception):
    """Splits out any error into the right error streams"""
    if logger is not None:
        logger.error(exception)
        logger.debug(format_exc())
    else:
        print exception, format_exc()


class ManagedProxyPush:
    """Class that holds all of the procedures/methods to push proxies and do
    necessary checks"""
    def __init__(self, logger=None):
        self.logger = logger

        try:
            assert self.check_user()
        except AssertionError:
            err = "This script must be run as {0}. Exiting.".format(
                should_runuser)
            raise AssertionError(err)

        self.locenv = environ.copy()
        self.locenv['KRB5CCNAME'] = KRB5CCNAME

        try:
            self.myjson = self.loadjson(inputfile)
        except Exception as e:
            err = 'Could not load json file.  Error is {0}'.format(e)
            raise Exception(err)

    @staticmethod
    def check_user():
        """Exit if user running script is not the authorized user"""
        runuser = getpwuid(geteuid())[0]
        print "Running script as {0}.".format(runuser)
        return runuser == should_runuser

    @staticmethod
    def loadjson(infile):
        """Load config from json file"""
        with open(infile, 'r') as proxylist:
            myjson = json.load(proxylist)
        return myjson

    def kerb_ticket_obtain(self):
        """Obtain a ticket based on the special use principal"""
        kerbcmd = ['/usr/krb5/bin/kinit', '-k', '-t',
                   '/opt/gen_keytabs/config/gcso_monitor.keytab',
                   'monitor/gcso/fermigrid.fnal.gov@FNAL.GOV']
        subprocess.check_call(kerbcmd, env=self.locenv)

    def check_keys(self, expt):
        """Make sure our JSON file has nodes and roles for the experiment"""
        if "roles" not in self.myjson[expt].keys() \
                or "nodes" not in self.myjson[expt].keys():
            err = "Error: input file improperly formatted for {0}" \
                  " (roles or nodes don't exist for this experiment)." \
                  " Please check ~rexbatch/gen_push_proxy/input_file.json" \
                  " on fifeutilgpvm01. I will skip this experiment for now." \
                  "\n".format(expt)
            error_handler(err)
            return False
        return True

    def check_output_mod(self, cmd):
        """A stripped-down version of subprocess.check_output in
        python 2.7+. Returns the stdout and return code"""
        process = subprocess.Popen(cmd, stdout=subprocess.PIPE,
                                   stderr=subprocess.STDOUT, env=self.locenv)
        output, _ = process.communicate()
        retcode = process.poll()
        if retcode:
            raise Exception(output)
        return output, retcode

    def get_proxy(self, expt, voms_role, account):
        """Get the proxy for the role and experiment

        Returns proxy path (outfile) if proxy was successfully generated
        """

        voms_prefix = '{0}:/'.format(expt) if expt in expt_vos else 'fermilab:/fermilab/'
        voms_string = '{0}{1}/Role={2}'.format(voms_prefix, expt, voms_role)

        outfile = account + '.' + voms_role + '.proxy'
        vpi_args = ["/usr/bin/voms-proxy-init", '-rfc', '-valid', '24:00', '-voms',
                    voms_string, '-cert' , CERT_BASE_DIR + '/' + account + '.cert',
                    '-key', CERT_BASE_DIR + '/' + account + '.key', '-out',
                    'proxies/' + outfile]

        # do voms-proxy-init now
        try:
            self.check_output_mod(vpi_args)
        except Exception:
            err = "Error obtaining {0}.  Please check the cert in {1} on " \
                  "fifeutilgpvm01. " \
                  "Continuing on to next role.".format(outfile, CERT_BASE_DIR)
            raise Exception(err)
        return outfile

    @staticmethod
    def check_node(node):
        """Pings the node to see if it's up or at least pingable"""
        pingcmd = ['ping', '-W', '5', '-c', '1', node]
        retcode = subprocess.call(pingcmd)
        return True if retcode == 0 else False

    def copy_proxy(self, node, account, expt, outfile):
        """Copies the proxies to submit nodes"""

        """ first we check the .k5login file to see if we're even allowed to push the proxy
        k5login_check = 'ssh ' + account + '@' + node + ' cat .k5login'
        nNames = -1
        """
        dest = account + '@' + node + ':' + self.myjson[expt]["dir"] + '/' + account + '/' + outfile
        newproxy = self.myjson[expt]["dir"] + '/' + account + '/' + outfile + '.new'
        oldproxy = self.myjson[expt]["dir"] + '/' + account + '/' + outfile
        scp_cmd = ['scp', '-o', 'ConnectTimeout=30', 'proxies/' + outfile, dest + '.new']
        chmod_cmd = ['ssh', '-ak', '-o', 'ConnectTimeout=30', account + '@' + node,
                     'chmod 400 {0} ; mv -f {1} {2}'.format(newproxy, newproxy, oldproxy)]

        try:
            self.check_output_mod(scp_cmd)
        except Exception as e:
            err = "Error copying ../proxies/{0} to {1}. " \
                  "Trying next node\n {2}".format(outfile, node, str(e))
            raise Exception(err)

        try:
            self.check_output_mod(chmod_cmd)
        except Exception as e:
            err = "Error changing permission of {0} to mode 400 on {1}. " \
                  "Trying next node\n {2}".format(outfile, node, str(e))
            raise Exception(err)

    def process_experiment(self, expt):
        """Function to process each experiment, including sending the proxy onto its nodes"""
        print 'Now processing ' + expt

        badnodes = []
        expt_success = True

        if not self.check_keys(expt): return False

        nodes = self.myjson[expt]["nodes"]

        # Ping nodes to see if they're up
        for node in nodes:
            if not self.check_node(node):
                self.logger.warning(
                    "The node {0} didn't return a response to ping after 5 "
                    "seconds.  Moving to the next node".format(node))
                expt_success = False
                badnodes.append(node)
                continue

        for roledict in self.myjson[expt]["roles"]:
            (role, acct), = roledict.items()
            try:
                outfile = self.get_proxy(expt, role, acct)
            except Exception as e:
                # We couldn't get a proxy - so just move to the next role
                expt_success = False
                error_handler(e)
                continue

            # OK, we got a ticket and a proxy, so let's try to copy
            for node in nodes:
                try:
                    self.copy_proxy(node, acct, expt, outfile)
                except Exception as e:
                    error_handler(e)
                    expt_success = False
                    if node in badnodes:
                        string = "Node {0} didn't respond to pings earlier - " \
                                 "so it's expected that copying there would fail.".format(node)
                        self.logger.warn(string)

        return expt_success

    def process_all_experiments(self):
        """Main execution method to process all experiments"""
        try:
            self.kerb_ticket_obtain()
        except Exception as e:
            err = 'WARNING: Error obtaining kerberos ticket; ' \
                  'may be unable to push proxies.  Error was {0}\n'.format(e)
            self.logger.warning(err)

        successful_expts = (expt for expt in self.myjson.iterkeys()
                            if self.process_experiment(expt))

        self.logger.info("This run completed successfully for the following "
                      "experiments: {0}.".format(', '.join(successful_expts)))


def main():
    """Main execution module"""
    global logger
    logger = setup_logger("Managed Proxy Push")
    try:
        m = ManagedProxyPush(logger)
    except Exception as e:
        error_handler(e)
        sys.exit(1)
    else:   # We instantiated the ManagedProxyPush class, with all its checks
        try:
            # Now let's actually process the experiments
            m.process_all_experiments()
        except Exception as e:
            error_handler(e)
            sys.exit(1)
    finally:
        try:
            # Get a line count for the tmp err file
            lc = sum(1 for _ in open(errfile,'r'))
            if lc != 0:
                sendslackmessage()
                sendemail()
            remove(errfile)
        except IOError:     # File doesn't exist - so no errors
            pass


if __name__ == '__main__':
    main()
    sys.exit(0)
