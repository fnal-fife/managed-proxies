#!/usr/bin/env python
import subprocess
from multiprocessing import Pool, current_process, TimeoutError, Process, Manager
import json
import yaml
import sys
import logging
import requests
from traceback import format_exc
from os import environ, geteuid, remove, listdir, mkdir, getcwd, rmdir
from shutil import move
import os.path
from pwd import getpwuid
import smtplib
import email.utils
from email.mime.text import MIMEText
from contextlib import contextmanager
import argparse
from time import sleep
from datetime import datetime

from QueueHandler import QueueHandler


# Global Variables
SOFT_TIMEOUT = 10
HARD_TIMEOUT = 300
INPUTFILE = 'proxy_push_config_test.yml'     # Default Config file
mainlogformatter = logging.Formatter(
    '%(asctime)s - %(name)s - %(levelname)s - %(message)s')
expt_format_string = '%(asctime)s - %(name)s - {expt} - %(levelname)s - %(message)s'
temp_log_dir = os.path.join(getcwd(), 'temp_log_dir')
expt_filename = os.path.join(temp_log_dir, 'log_test_{expt}.log')
file_timestamp = datetime.strftime(datetime.now(), "%Y-%m-%d_%H:%M:%S")

# Functions
# Set up
def parse_arguments():
    """
    Parse arguments to this script
    
    :return Namespace:  Namespace of assigned arguments
    """
    parser = argparse.ArgumentParser()
    parser.add_argument("-e", "--experiment", type=str,
                        help="Push for a single experiment")
    parser.add_argument("-c", "--config", type=str,
                        help="Alternate config file", default=INPUTFILE)
    parser.add_argument("-t", "--test", action="store_true",
                        help="Test mode", default=False)
    return parser.parse_args()


def load_config(infile, test=False):
    """
    Load config into dict from config file
    
    :param str infile:  Configuration file for Managed Proxies Service
    :param bool test:  Test mode on (True) or off (False)
    :return dict: Adjusted configuration dictionary for use by rest of module
    """
    with open(infile, 'r') as f: config = yaml.load(f)

    # If we're running test, use test parameters
    if test: config['notifications'] = config['notifications_test']

    del config['notifications_test']
    return config


def check_user(testuser):
    """Tests if user running script is not the authorized user
    
    :param str testuser: Username that the script must run as
    :return bool: Whether or not the user running the script is the authorized user
    """
    runuser = getpwuid(geteuid())[0]
    print "Running script as {0}.".format(runuser)
    return runuser == testuser


def kerb_ticket_obtain(krb5ccname):
    """
    Obtain a ticket based on the special use principal
    
    :param str krb5ccname: What the env variable KRB5CCNAME should be set to before running kinit
    :return dict:  Environment of running process.  Can be passed to other processes
    """
    locenv = environ.copy()
    locenv['KRB5CCNAME'] = krb5ccname

    kerbcmd = ['/usr/krb5/bin/kinit', '-k', '-t',
               '/opt/gen_keytabs/config/gcso_monitor.keytab',
               'monitor/gcso/fermigrid.fnal.gov@FNAL.GOV']
    subprocess.check_call(kerbcmd, env=locenv)
    return locenv


# Main Logger
def main_logger(queue, config):
    """
    Set up the main logger, listen for messages in the queue, and log them

    :param multiprocessing.Manager.Queue queue:  Multiprocessing queue that the 
        main logger listens to
    :param dict config: Configuration value dictionary from config file    
    """
    mainlogger = logging.getLogger('Main log')
    mainlogger.setLevel(logging.DEBUG)

    # Stream Handler to stdout
    sh = logging.StreamHandler()
    sh.setLevel(logging.INFO)

    # File Handler to main logfile
    fh = logging.FileHandler(config['logs']['logfile'])
    fh.setLevel(logging.DEBUG)

    # File Handler to temporary error file (used to email errors to FIFE group)
    eh = logging.FileHandler(config['logs']['errfile'])
    eh.setLevel(logging.WARNING)

    for handler in (sh, fh, eh):
        mainlogger.addHandler(handler)        
        handler.setFormatter(mainlogformatter)

    # for handler in mainlogger.handlers:
        # handler.setFormatter(mainlogformatter)

    mainlogger.info("Starting new run")
    while True:
        try:
            q_msg = queue.get()
            if q_msg is None:     # Poison pill
                mainlogger.info("Main Logger shutting down")
                break
            if isinstance(q_msg, tuple) and len(q_msg) == 3:
                expt, level, msg = q_msg
                
                # Sanity check - are we actually passing in something that could be a logging level?
                assert isinstance(level, int)       
                
                mainlogger.log(level, msg)
            else:
                mainlogger.handle(q_msg)
        except Exception as e:
            mainlogger.error(e)


def kill_main_logger(queue):
    """Kill the main logger process by putting the "poison pill" in the queue
    :param multiprocessing.Manager.Queue queue: Queue to put the "poison pill" in
    """
    queue.put(None)
    sleep(0.1)


# Set up and run worker job
def run_worker(expt, config, log_queue, environment):
    """
    Run a worker for an experiment.

    :param str expt:  Experiment for which proxies are being pushed
    :param dict config:  Configuration value dictionary from config file
    :param multiprocessing.Manager.Queue log_queue: Queue to dump log messages into
    :param dict environment: Environment of parent process to pass into workers
    :return: Return expt if successful, None if not
    """
    try:
        expt_push = ManagedProxyPush(config, expt, log_queue, environment)
    except Exception:
        return None
    else:   # We instantiated the ManagedProxyPush class, with all its checks
        try:
            # Now let's actually process the experiment
            assert expt_push.process_experiment()
        except Exception:
            return None
        else:
            return expt


class ManagedProxyPush(object):
    """
    Class that holds all of the procedures/methods to push proxies and do
    necessary checks.  Runs within a process.

    :param dict config: Configuration value dictionary from config file
    :param str expt: Experiment this instance will be pushing proxies for
    :param multiprocessing.Manager.Queue msg_queue: Message queue to use when
        setting up QueueHandler
    :param dict environment:  Environment to use when running shell commands
    """

    def __init__(self, config, expt, msg_queue, environment):
        self.config = config
        self.locenv = environment
        self.queue = msg_queue
        self.expt = expt

        self.expt_filename = expt_filename.format(expt=self.expt)

        for key, item in self.config['global'].iteritems():
            setattr(self, key, item)

        self.logger = self.expt_logger()

        # Make sure our config file has the necessary items to process the experiment
        try:
            assert self.check_keys()
        except AssertionError as e:
            self.logger.error(e)
            raise

    def expt_logger(self):
        """
        Set up the logger for this class.  We add an experiment-specific
        file handler so that experiment-specific error emails can be sent

        :return logging.Logger: Logger for this instance
        """
        exptlogger = logging.getLogger(self.expt)
        exptlogger.setLevel(logging.DEBUG)
        exptformatter = logging.Formatter(expt_format_string.format(expt=self.expt))

        # File handler for experiment-specific error file
        h = logging.FileHandler(self.expt_filename)
        h.setLevel(logging.WARNING)
        h.setFormatter(exptformatter)

        # Custom QueueHandler to put all log messages into our log queue
        qh = QueueHandler(self.queue)
        qh.setFormatter(exptformatter)

        for handler in (h, qh):
            exptlogger.addHandler(handler)

        return exptlogger

    def check_keys(self):
        """Make sure our JSON file has nodes and roles for the experiment
        
        :raises AssertionError: If our test fails, this is raised
        :return bool: Returns True if test is passed
        """
        if "roles" not in self.config['experiments'][self.expt].keys() or "nodes" not in self.config['experiments'][self.expt].keys(): 
            err = "Error: input file improperly formatted for {0}" \
                " (roles or nodes don't exist for this experiment)." \
                " Please check the config file" \
                " on fifeutilgpvm01. I will skip this experiment for now." \
                "\n".format(self.expt)
            raise AssertionError(err)
        return True

    @staticmethod
    def check_node(node):
        """Pings the node to see if it's up or at least pingable
        
        :param str node: Node to ping
        :return bool:  True if ping returns 0, False otherwise
        """
        pingcmd = ['ping', '-W', '5', '-c', '1', node]
        retcode = subprocess.call(pingcmd)
        return True if retcode == 0 else False

    def get_proxy(self, voms_role, account):
        """Get the proxy for the role and experiment

        :param str voms_role: VOMS role for desired proxy
        :param str account: UNIX account for desired proxy
        :return str: Proxy path (outfile) if proxy was successfully generated
        """
        voms_prefix = self.config['experiments'][self.expt]['vomsgroup'] \
            if 'vomsgroup' in self.config['experiments'][self.expt] \
            else 'fermilab:/fermilab/{0}/'.format(self.expt)
        voms_string = '{0}Role={1}'.format(voms_prefix, voms_role)

        certfile = self.config['experiments'][self.expt]['certfile'] \
            if 'certfile' in self.config['experiments'][self.expt] \
            else os.path.join(self.CERT_BASE_DIR, '{0}.cert'.format(account))
        keyfile = self.config['experiments'][self.expt]['keyfile'] \
            if 'keyfile' in self.config['experiments'][self.expt] \
            else os.path.join(self.CERT_BASE_DIR, '{0}.key'.format(account))

        outfile = '{0}.{1}.proxy'.format(account, voms_role)
        outfile_path = os.path.join('proxies', outfile)

        vpi_args = ["/usr/bin/voms-proxy-init", '-rfc', '-valid', '24:00', '-voms',
                    voms_string, '-cert', certfile,
                    '-key', keyfile, '-out', outfile_path]

        # Do voms-proxy-init now
        try:
            check_output_mod(vpi_args, self.locenv)
        except Exception as e:
            err = "Error obtaining {0}.  Please check the cert on " \
                  "fifeutilgpvm01. \n{1}" \
                  "Continuing on to next role.".format(outfile, e)
            raise Exception(err)
        return outfile

    def copy_proxy(self, node, account, srcfile):
        """Copies the proxies to submit nodes
        
        :param str node: Destination node to copy proxy to
        :param str account: UNIX account to log into node as
        :param str srcfile: Filename of proxy to copy out
        """
        account_node = '{0}@{1}.fnal.gov'.format(account, node)
        srcpath = os.path.join('proxies', srcfile)
        newproxypath = os.path.join(
            self.config['experiments'][self.expt]["dir"], account, '{0}.new'.format(srcfile))
        finalproxypath = os.path.join(
            self.config['experiments'][self.expt]["dir"], account, srcfile)

        scp_cmd = ['scp', '-o', 'ConnectTimeout=30', srcpath,
                   '{0}:{1}'.format(account_node, newproxypath)]
        chmod_cmd = ['ssh', '-o', 'ConnectTimeout=30', account_node,
                     'chmod 400 {0} ; mv -f {0} {1}'.format(newproxypath, finalproxypath)]

        try:
            check_output_mod(scp_cmd, self.locenv)
        except Exception as e:
            err = "Error copying ../proxies/{0} to {1}. " \
                  "Trying next node\n{2}".format(srcfile, node, str(e))
            raise Exception(err)

        try:
            check_output_mod(chmod_cmd, self.locenv)
        except Exception as e:
            err = "Error changing permission of {0} to mode 400 on {1}. " \
                  "Trying next node \n{2}".format(srcfile, node, str(e))
            raise Exception(err)

    def process_experiment(self):
        """
        Function to process each experiment.  Checks an experiment's nodes, generates the proxies, and 
        sends them to the nodes

        :return bool: Whether or not processing the experiment succeeded with no errors
        """
        self.logger.info('Now processing {0}'.format(self.expt))
        badnodes = []
        expt_success = True     # Flag that we'll change if there are any errors

        nodes = self.config['experiments'][self.expt]['nodes']

        # Ping nodes to see if they're up
        for node in nodes:
            if not self.check_node(node):
                self.logger.warning(
                    "The node {0} didn't return a response to ping after 5 "
                    "seconds.  Please investigate, and see if the node is up. "
                    "It may be necessary for the experiment to request via a ServiceNow ticket "
                    "that the Scientific Server Infrastructure group reboot "
                    "the node. Moving to the next node".format(node))
                expt_success = False
                badnodes.append(node)
                continue

        for roledict in self.config['experiments'][self.expt]['roles']:
            (role, acct), = roledict.items()
            try:
                outfile = self.get_proxy(role, acct)
            except Exception as e:
                # We couldn't get a proxy - so just move to the next role
                expt_success = False
                self.logger.error(e)
                continue

            # OK, we got a ticket and a proxy, so let's try to copy
            for node in nodes:
                try:
                    self.copy_proxy(node, acct, outfile)
                except Exception as e:
                    self.logger.error(e)
                    expt_success = False
                    if node in badnodes:
                        msg = "Node {0} didn't respond to pings earlier - " \
                            "so it's expected that copying there would fail.".format(
                                node)
                        self.logger.warn(msg)

        return expt_success


# Cleanup actions
def cleanup_global(config, queue):
    """
    Clean up for the main process.  Removes/archives temporary files, sends 
    general emails and Slack messages.
    
    :param dict config: Configuration value dictionary from config file    
    :param multiprocessing.Manager.Queue queue: queue to put any messages in
    """
    # Remove the temp log dir
    try:
        rmdir(temp_log_dir)
    except OSError:
        queue.put((None, logging.WARN, "{0} is not empty - not removing directory".format(temp_log_dir)))

    # Send general notifications
    errfile = config['logs']['errfile']
    lc = sum(1 for _ in open(errfile, 'r'))
    if lc > 0:
        sendemail(config, queue)
        sendslackmessage(config, queue)
        queue.put((None, logging.INFO, "Sent notifications successfully"))
    else:
        queue.put((None, logging.INFO, "No notifications to send"))

    # kill_main_logger(queue)       IF THINGS FAIL COME BACK HERE

    # Remove the temporary error file
    try:
        remove(errfile)
    except Exception as e:
        # print "Could not remove temporary error file. {0}".format(e)
        queue.put((None, logging.ERROR, "Could not remove temporary error file. \n{0}".format(e)))


def cleanup_expt(expt, queue, config):
    """
    Cleanup for each experiment process.  Sends experiment-specific email, 
    and removes temporary file.  If that fails, we archive it for further
    troubleshooting. 

    :param str expt:  Experiment's process to clean up
    :param multiprocessing.Manager.Queue queue: queue to put any messages in
    :param dict config: Configuration value dictionary from config file
    """
    logargs = [expt, queue]
    queue.put((expt, logging.DEBUG, "Cleaning up {0}".format(expt)))
    filename = expt_filename.format(expt=expt)

    lc = sum(1 for _ in open(filename, 'r'))

    try:
        if lc != 0:  sendemail(config, queue, expt)
    except Exception as e:
        msg = "Error sending email for experiment {0}.  {1}".format(expt, e)
        queue.put((expt, logging.ERROR, msg))
        
    smsg = "Cleaned up {0} with no issues".format(expt)
    try:
        remove(filename)
    except OSError:
        # File doesn't exist
        msg = "Filename {0} doesn't exist.  There were probably no errors in that experiment's run".format(filename)
        queue.put((expt, logging.DEBUG, msg))
        queue.put((expt, logging.DEBUG, smsg))
    except Exception as e:
        try:
            newfilename = '{0}{1}'.format(filename, file_timestamp)
            move(filename, newfilename)
            msg = "Was not able to remove experiment {0} logfile.  Moved to \
                archive for further troubleshooting by Distributed Computing Support.  Error was {1}".format(expt, e)
            queue.put((expt, logging.WARN, msg))
        except Exception as e:
            emsg = "Could not move logfile.  Someone from Distributed Computing Support \
                should look into this.  Error was {0}".format(e)
            queue.put((expt, logging.ERROR, emsg))
    else:
        queue.put((expt, logging.DEBUG, smsg))
        queue.put((expt, logging.INFO, "Done with {0}".format(expt)))


# Notification actions
def sendemail(config, log_queue, expt=None):
    """
    Function to send email after error file is populated.
    
    :param dict config: Configuration value dictionary from config file
    :param multiprocessing.Manager.Queue log_queue: queue to put any messages in
    :param str expt:  Experiment we're sending email for.  If this is None, we send the 
        general emails to the FIFE group
    """
    error_file = config['logs']['errfile'] if expt is None else expt_filename.format(
        expt=expt)

    with open(error_file, 'r') as f: message = f.read()

    info_msg = "\n\nWe've compiled a list of common errors here: "\
        "https://cdcvs.fnal.gov/redmine/projects/fife/wiki/Common_errors_with_Managed_Proxies_Service. "\
        "\n\nIf you have any questions or comments about these emails, "\
        "please open a Service Desk ticket to the Distributed Computing "\
        "Support group." if expt is not None else ''
    message += info_msg

    sender = 'fife-group@fnal.gov'
    to = config['notifications']['admin_email'] if expt is None else config['experiments'][expt]['emails']
    msg = MIMEText(message)

    if expt is None:
        msg['To'] = email.utils.formataddr(('FIFE GROUP', to))
    else:
        msg['To'] = ', '.join(to)

    msg['From'] = email.utils.formataddr(('FIFE GROUP', sender))

    expt_subj_string = " for {0}".format(expt) if expt is not None else ""
    msg['Subject'] = "Managed Proxy Push errors{0}".format(expt_subj_string)

    try:
        smtpObj = smtplib.SMTP('smtp.fnal.gov')
        smtpObj.sendmail(sender, to, msg.as_string())
        smsg = "Successfully sent notification email to {0}".format(to)
        log_queue.put((expt, logging.INFO, smsg))
    except Exception as e:
        err = "Error:  unable to send email.\n{0}\n".format(e)
        log_queue.put((expt, logging.ERROR, err))


def sendslackmessage(config, log_queue):
    """
    Function to send notifications to fife-group #alerts slack channel
    from temporary error file
    
    :param dict config: Configuration value dictionary from config file
    :param multiprocessing.Manager.Queue log_queue: queue to put any messages in
    """
    with open(config['logs']['errfile'], 'r') as f:
        payloadtext = f.read()

    payload = {"text": payloadtext}
    headers = {"Content-type": "application/json"}

    try:
        r = requests.post(config['notifications']['SLACK_ALERTS_URL'], data=json.dumps(payload),
                          headers=headers)
        r.raise_for_status()
    except Exception as e:
        errmsg = "Could not send slack message.  " \
                 "Status code {0}, response text {1}".format(
                     r.status_code, r.text)
        log_queue.put((None, logging.ERROR, errmsg))


# I'm mostly sure this is redundant
# def send_general_notifications(config, log_queue, test=False):
#     """
#     Function to send all general notifications
    
    
#     """
#     # General email and Slack
#     try:
#         # Get a line count for the tmp err file
#         errfile = config['logs']['errfile']
#         lc = sum(1 for _ in open(errfile, 'r'))
#         if lc != 0:
#             exists_error = True
#             sendslackmessage(config, log_queue)
#             sendemail(config, log_queue)
#         # remove(errfile)       # I think this is redundant
#     except IOError:     # File doesn't exist - so no errors
#         pass

#     msg = "All notifications sent" if exists_error else "No notifications to send"
#     log_queue.put((None, logging.INFO, msg))


# Miscellaneous

def check_output_mod(cmd, locenv):
    """A stripped-down version of subprocess.check_output in
    python 2.7+. Returns the stdout and return code"""
    process = subprocess.Popen(cmd, stdout=subprocess.PIPE,
                                stderr=subprocess.STDOUT, env=locenv)
    output, _ = process.communicate()
    retcode = process.poll()
    if retcode:
        raise Exception(output)
    return output, retcode


# The function that runs everything
def run_push(args, config, log_msg_queue):
    """Main execution module.  Starts up the process pool, assigns tasks to
        it (one per experiment), and handles timeouts and other errors for 
        processes.  The only reason this isn't "main" is because I needed to 
        pass arguments into it so I can handle exceptions outside of this 
        function.  Otherwise, exceptions here would lead to "Broken Pipe" getting 
        dumped in an infinite loop into stdout and the logfile.
    
    :param Namespace args:  Namespace of arguments passed to script
    :param dict config: Configuration value dictionary from config file
    :param multiprocessing.Manager.Queue log_msg_queue: queue to put any messages in
    """
    successful_experiments = []
    failed_experiments = []
    second_try = {}

    if args.test: log_msg_queue.put((None, logging.INFO, "Running in test mode"))
    log_msg_queue.put((None, logging.INFO, "Using config file {0}".format(args.config)))

    # Are we running as the right user?
    try:
        assert check_user(config['global']['should_runuser'])
    except AssertionError:
        msg = "Script must be run as {0}. Exiting.".format(
            config['global']['should_runuser'])
        log_msg_queue.put((None, logging.ERROR, msg))
        # kill_main_logger(log_msg_queue) # Shouldn't need this
        sys.exit(1)

    # Setup temp log dir
    try: listdir(temp_log_dir)
    except OSError: mkdir(temp_log_dir)

    # Get a kerb ticket
    try:
        locenv = kerb_ticket_obtain(config['global']['KRB5CCNAME'])
    except Exception as e:
        w = 'WARNING: Error obtaining kerberos ticket; ' \
                'may be unable to push proxies.  Error was {0}\n'.format(e)
        log_msg_queue.put((None, logging.WARN, w))

    # Set up the worker pool
    numworkers = 1 if args.experiment else 5
    expts = [args.experiment,] if args.experiment \
        else config['experiments'].keys()

    pool = Pool(processes=numworkers)

    # Start workers, send jobs to the processes    
    results = {}
    for expt in expts:
        try:
            results[expt] = pool.apply_async(run_worker, (expt, config, log_msg_queue, locenv))
        except Exception as e:
            log_msg_queue.put((expt, logging.ERROR, e))
            continue 

    # If we ever upgrade to python 2.7...
    #   results = {expt: pool.apply_async(run_worker, (expt, config, log_msg_queue))}
    
    pool.close()

    def try_expt_process(expt, try_expt, round_no):
        """
        Helper function that sorts experiment results into successes and failures, 
        and handles each

        :param str expt:  Experiment we're examining
        :param str try_expt:  If the process succeeded try_expt = expt.  Otherwise, it's None
        :param round_no:  Which round we're in
        """
        if try_expt: 
            msg = "{0} finished successfully in round {1}.".format(expt, round_no)
            log_msg_queue.put((expt, logging.DEBUG, msg))
            successful_experiments.append(try_expt)
        else:
            msg = "{0} failed in round {1}.".format(expt, round_no)
            log_msg_queue.put((expt, logging.DEBUG, msg))
            failed_experiments.append(expt)

    # First round
    for expt, result in results.iteritems():
        try:
            try_expt = result.get(timeout=SOFT_TIMEOUT)
            try_expt_process(expt, try_expt, 1)
            cleanup_expt(expt, log_msg_queue, config)
        except TimeoutError:
            msg = "{0} hit the soft timeout.  Will try to get result in next round".format(expt)
            second_try[expt] = result
            log_msg_queue.put((expt, logging.DEBUG, msg))
        except Exception as e:
            log_msg_queue.put((expt, logging.ERROR, e))
            failed_experiments.append(expt)
            try:
                cleanup_expt(expt, log_msg_queue, config)
            except Exception as e:
                msg = "Could not cleanup {0}. \n{1}".format(expt, e)
                log_msg_queue.put( (None, logging.ERROR, msg))

    # Second round
    for expt, result in second_try.iteritems():
        try:
            try_expt = result.get(timeout=HARD_TIMEOUT)
            try_expt_process(expt, try_expt, 2)
        except TimeoutError:
            msg = "{0} hit the hard timeout".format(expt)
            log_msg_queue.put((expt, logging.ERROR, msg))
            failed_experiments.append(expt)
        except Exception as e:
            log_msg_queue.put((expt, logging.ERROR, e))
            failed_experiments.append(expt)
        finally:
            try:
                cleanup_expt(expt, log_msg_queue, config)
            except Exception as e:
                pool.terminate()
                pool.join()
                msg = "Could not cleanup {0}. \n{1}".format(expt, e)
                log_msg_queue.put( (None, logging.ERROR, msg))

    pool.terminate()
    pool.join()

    log_msg_queue.put((None, logging.INFO,"Successful Experiments: {0}".format(successful_experiments)))
    log_msg_queue.put((None, logging.INFO, "Failed Experiments: {0}".format(failed_experiments)))
    
    cleanup_global(config, log_msg_queue)


def main():
    """Set up the entire module, and handle any error so that we don't get the Broken Pipe madness mentioned 
    in the docstring for run_push.
    """
    def pre_queue_exception_action(err, e):
        """
        Handle error messages before we've started up our main log.

        :param str err:  Custom Error message
        :param str e:  Actual error thrown by code
        """
        er = "{0}.\n{1}".format(err, e)
        print er
        sys.exit(1)

    try:
        args = parse_arguments()
    except Exception as e:
        # err = 'Could not parse arguments. \n{0}'.format(e)
        # print err
        # sys.exit(1)
        pre_queue_exception_action('Could not parse arguments', e)

    try:
        config = load_config(args.config, args.test)
    except Exception as e:
        # err = 'Could not load config file. \n{0}'.format(e)
        # print err
        # sys.exit(1)
        pre_queue_exception_action('Could not load config file', e)

    try:
        # Main Log queue and listener process
        m = Manager()
        listener_queue = m.Queue()
    except Exception as e:
        # err = 'Could not initialize log queue. \n{0}'.format(e)
        # print err
        # sys.exit(1)
        pre_queue_exception_action('Could not initialize log queue', e)
    
    try: 
        listener = Process(target=main_logger, args=(listener_queue, config))
        listener.start()
    except Exception as e:
        # err = 'Could not start log queue. \n{0}'.format(e)
        # print err
        # sys.exit(1)
        pre_queue_exception_action('Could not start log listener process', e)

    try:
        run_push(args, config, listener_queue)
    except Exception as e:
        listener_queue.put((None, logging.ERROR, e))
        kill_main_logger(listener_queue)
        listener.join()
        sys.exit(1)
    else:
        kill_main_logger(listener_queue)
        listener.join()
        sys.exit(0)


if __name__ == '__main__':
    main()
