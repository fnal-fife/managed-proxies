#!/usr/bin/env python
import subprocess
from multiprocessing import Pool, current_process, TimeoutError, Process, Manager
import json
import yaml
import sys
import logging
import requests
from logging.handlers import RotatingFileHandler
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
SOFT_TIMEOUT = 60
HARD_TIMEOUT = 100      # Make it 300 in production
inputfile = 'proxy_push_config.yml'     # Default Config file
# logger = None
mainlogformatter = logging.Formatter(
    '%(asctime)s - %(name)s - %(levelname)s - %(message)s')
expt_format_string = '%(asctime)s - %(name)s - {expt} - %(levelname)s - %(message)s'
temp_log_dir = os.path.join(getcwd(), 'temp_log_dir')
expt_filename = os.path.join(temp_log_dir, 'log_test_{expt}.log')
file_timestamp = datetime.strftime(datetime.now(), "%Y-%m-%d_%H:%M:%S")

# expt_files = {}             # Experiment log file dict
# config = None               # Global configuration

# Functions

# Set up

def load_config(infile, test=False):
    """Load config into dict from config file"""
    # global config
    with open(infile, 'r') as f: config = yaml.load(f)

    # If we're running test, use test parameters
    if test: config['notifications'] = config['notifications_test']

    del config['notifications_test']
    return config


def parse_arguments():
    """Parse arguments to this script"""
    parser = argparse.ArgumentParser()
    parser.add_argument("-e", "--experiment", type=str,
            help="Push for a single experiment")
    parser.add_argument("-c", "--config", type=str,
            help="Alternate config file", default=inputfile)
    parser.add_argument("-t", "--test", action="store_true",
            help="Test mode", default=False)
    return parser.parse_args()


def check_user(testuser):
    """Exit if user running script is not the authorized user"""
    runuser = getpwuid(geteuid())[0]
    print "Running script as {0}.".format(runuser)
    return runuser == testuser


def kerb_ticket_obtain(krb5ccname):
    """Obtain a ticket based on the special use principal"""
    locenv = environ.copy()
    locenv['KRB5CCNAME'] = krb5ccname

    kerbcmd = ['/usr/krb5/bin/kinit', '-k', '-t',
               '/opt/gen_keytabs/config/gcso_monitor.keytab',
               'monitor/gcso/fermigrid.fnal.gov@FNAL.GOV']
    subprocess.check_call(kerbcmd, env=locenv)


# # Handling logfiles
# def setup_logger(name):
#     """Sets up the logger"""
#     global logger, expt_files

#     handler_list = []
#     logfile = config['logs']['logfile']
#     errfile = config['logs']['errfile']

#     # Create Logger
#     logger = logging.getLogger(name)
#     logger.setLevel(logging.DEBUG)

#     # Console handler - info
#     ch = logging.StreamHandler()
#     ch.setLevel(logging.INFO)
#     handler_list.append(ch)

#     # Logfile Handler
#     lh = RotatingFileHandler(logfile, maxBytes=2097152, backupCount=3)
#     lh.setLevel(logging.DEBUG)
#     logfileformat = logging.Formatter(
#         "%(asctime)s - %(name)s - %(levelname)s - %(message)s")
#     lh.setFormatter(logfileformat)
#     handler_list.append(lh)

#     # Error file
#     eh = logging.FileHandler(errfile)
#     eh.setLevel(logging.WARNING)
#     errfileformat = logging.Formatter(
#         "%(asctime)s - %(levelname)s - %(message)s")
#     eh.setFormatter(errfileformat)
#     handler_list.append(eh)

#     for handler in handler_list:
#         logger.addHandler(handler)

#     return logger


# @contextmanager
# def expt_log_active(logger, expt, level=None, close=True):
#     """Context manager to temporarily add an experiment-specific log handler to the logger"""
#     global expt_files

#     # __init__ and __enter__ code
#     filename = expt_files[expt]
#     h = logging.FileHandler(filename)
#     if level is not None:
#         h.setLevel(level)
#     logger.addHandler(h)

#     yield

#     # __exit__ code
#     if h is not None:
#         logger.removeHandler(h)
#     if h is not None and close:
#         h.close()


# def remove_expt_logs():
#     """Removes all the temporary experiment logfiles"""
#     global logger, expt_files
#     for f in expt_files.itervalues():
#         try:
#             remove(f)
#         except OSError as e:
#             error_handler(e)


# Error handling
# def error_handler(exception):
#     """Splits out any error into the right error streams"""
#     if logger is not None:
#         logger.error(exception)
#         logger.debug(format_exc())
#     else:
#         print exception, format_exc()

# Main Logger

def main_logger(queue, config):
    # mainlog
    mainlogger = logging.getLogger('Main log')
    mainlogger.setLevel(logging.DEBUG)
    # maybe use a context filter to add expt info
    sh = logging.StreamHandler()
    sh.setLevel(logging.INFO)
    fh = logging.FileHandler(config['logs']['logfile'])
    fh.setLevel(logging.DEBUG)
    eh = logging.FileHandler(config['logs']['errfile'])
    eh.setLevel(logging.WARNING)

    for handler in (sh, fh, eh):
        mainlogger.addHandler(handler)

    # mainlogger.addHandler(sh)
    # mainlogger.addHandler(fh)

    for handler in mainlogger.handlers:
        handler.setFormatter(mainlogformatter)

    mainlogger.info("\nStarting new run")
    while True:
        try:
            q_msg = queue.get()
            if q_msg is None:     # Poison pill
                mainlogger.info("Main Logger shutting down")
                break
            if isinstance(q_msg, tuple) and len(q_msg) == 3:
                expt, level, msg = q_msg
                assert isinstance(level, int)       # Sanity check - are we actually passing in something that could be a logging level?
                mainlogger.log(level, msg)
            else:
                mainlogger.handle(q_msg)
        except Exception as e:
            mainlogger.error(e)


def kill_main_logger(queue):
    queue.put(None)
    sleep(0.1)



# Set up and run worker job

def run_worker(expt, config, log_queue):
    try:
        expt_push = ManagedProxyPush(config, expt, log_queue)
    except Exception as e:
        log_queue.put((expt, logging.ERROR, e))
        # error_handler(e)
        sys.exit(1)
    else:   # We instantiated the ManagedProxyPush class, with all its checks
        try:
            # Now let's actually process the experiment
            expt_push.process_experiment()
        except Exception as e:
            log_queue.put((expt, logging.ERROR, e))
        else:
            return expt


# Cleanup actions

def cleanup_global(config, queue): 
    # Verify general cleanup actions
    # - send email to us,
    # - send slack to us
    # - kill main logger
    # Send FIFE email of error file

    # Remove the temp log dir
    try:
        rmdir(temp_log_dir)
    except OSError:
        queue.put((None, logging.WARN, "{0} is not empty - not removing directory".format(temp_log_dir)))

    # Send general notifications
    sendemail(config, queue)
    sendslackmessage(config, queue)

    kill_main_logger(queue)

    # Remove the temporary error file
    try:
        remove(config['logs']['errfile'])
    except Exception as e:
        print "Could not remove temporary error file. {0}".format(e)


def cleanup_expt(expt, queue, config): 
    # Various cleanup actions
    logargs = [expt, queue]
    queue.put((expt, logging.INFO, "Cleaning up {0}".format(expt)))
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
        queue.put((expt, logging.INFO, msg))
        queue.put((expt, logging.INFO, smsg))
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
        queue.put((expt, logging.INFO, smsg))



# Notification actions
def sendemail(config, log_queue, expt=None):
    """Function to send email after message string is populated."""
    # global config, expt_files
    error_file = config['logs']['errfile'] if expt is None else expt_filename.format(
        expt=expt)

    with open(error_file, 'r') as f:
        message = f.read()

    info_msg = "We've compiled a list of common errors here: "\
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
        # if logger is not None:
        #     logger.info(smsg)
    except Exception as e:
        err = "Error:  unable to send email.\n{0}\n".format(e)
        log_queue.put((expt, logging.ERROR, err))
        # if logger is not None:
        #     error_handler(err)
        # else:
        #     print err
        # raise


def sendslackmessage(config, log_queue):
    """Function to send notification to fife-group #alerts slack channel"""
    with open(config['logs']['errfile'], 'r') as f:
        payloadtext = f.read()

    payload = {"text": payloadtext}
    headers = {"Content-type": "application/json"}

    try:
        r = requests.post(config['notifications']['SLACK_ALERTS_URL'], data=json.dumps(payload),
                          headers=headers)
        r.raise_for_status()
    except Exception as e:
        # if r.status_code != requests.codes.ok:
        errmsg = "Could not send slack message.  " \
                 "Status code {0}, response text {1}".format(
                     r.status_code, r.text)
        log_queue.put((None, logging.ERROR, errmsg))
        # if logger is not None:
        #     error_handler(errmsg)
        # else:
        #     print errmsg


def send_general_notifications(config, log_queue, test=False):
    """Function to send all notifications"""
    # global expt_files   # , logger
    # expt_files_to_keep = []
    # exists_error = False

    # if not test:
    #     # Experiment-specific emails if it's not a test run
    #     for expt, f in expt_files.iteritems():
    #         # Get line count for experiment-specific log file
    #         lc = sum(1 for _ in open(f, 'r'))
    #         if lc != 0:
    #             exists_error = True
    #             try:
    #                 sendemail(expt)
    #             except Exception as e:
    #                 error_handler(e)    # Don't exit - just move to the next error file
    #                 expt_files_to_keep.append(expt)

    #     for e in expt_files_to_keep: del expt_files[e]

    # remove_expt_logs()   # Uses expt_files to find what files to delete

    # General email and Slack
    try:
        # Get a line count for the tmp err file
        errfile = config['logs']['errfile']
        lc = sum(1 for _ in open(errfile, 'r'))
        if lc != 0:
            exists_error = True
            sendslackmessage(config, log_queue)
            sendemail(config, log_queue)
        remove(errfile)
    except IOError:     # File doesn't exist - so no errors
        pass

    msg = "All notifications sent" if exists_error else "No notifications to send"
    # logger.info(msg)
    log_queue.put((None, logging.INFO, msg))


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


# Pushing proxy
class ManagedProxyPush:
    """Class that holds all of the procedures/methods to push proxies and do
    necessary checks"""
    def __init__(self, config, expt, msg_queue):
        self.config = config
        # self.expt_config = self.config['experiments'][self.expt]

        self.queue = msg_queue
        self.expt = expt

        self.expt_filename = expt_filename.format(expt=self.expt)

        for key, item in self.config['global'].iteritems():
            setattr(self, key, item)
        
        self.logger = self.expt_logger()
        
        try:
            assert self.check_keys()
        except AssertionError as e:
            self.logger.error(e)




        # try:
        #     assert self.check_user(self.should_runuser)
        # except AssertionError:
        #     err = "This script must be run as {0}. Exiting.".format(self.should_runuser)
        #     raise AssertionError(err)


        # # grab the initial environment
        # self.locenv = environ.copy()
        # self.locenv['KRB5CCNAME'] = self.KRB5CCNAME

        # try:
        #     self.kerb_ticket_obtain()
        # except Exception as e:
        #     err = 'WARNING: Error obtaining kerberos ticket; ' \
        #           'may be unable to push proxies.  Error was {0}\n'.format(e)
        #     self.logger.warning(err)

    

    # def kerb_ticket_obtain(self):
    #     """Obtain a ticket based on the special use principal"""
    #     kerbcmd = ['/usr/krb5/bin/kinit', '-k', '-t',
    #                '/opt/gen_keytabs/config/gcso_monitor.keytab',
    #                'monitor/gcso/fermigrid.fnal.gov@FNAL.GOV']
    #     subprocess.check_call(kerbcmd, env=self.locenv)

    def expt_logger(self):
        filename = self.expt_filename
        exptlogger = logging.getLogger(self.expt)
        exptlogger.setLevel(logging.ERROR)
        exptformatter = logging.Formatter(
            expt_format_string.format(expt=self.expt))
        
        h = logging.FileHandler(filename)
        h.setFormatter(exptformatter)

        qh = QueueHandler(self.queue)
        qh.setFormatter(exptformatter)

        for handler in (h, qh): exptlogger.addHandler(handler)
        # exptlogger.addHandler(h)
        # exptlogger.addHandler(qh)
        return exptlogger

    def check_keys(self):
        """Make sure our JSON file has nodes and roles for the experiment"""
        if "roles" not in self.config['experiments'][self.expt].keys() or "nodes" not in self.config['experiments'][self.expt].keys():
            err = "Error: input file improperly formatted for {0}" \
                " (roles or nodes don't exist for this experiment)." \
                " Please check the config file" \
                " on fifeutilgpvm01. I will skip this experiment for now." \
                "\n".format(self.expt)
            raise AssertionError(err)
            

        # if "roles" not in self.config['experiments'][expt].keys() \
        #         or "nodes" not in self.config['experiments'][expt].keys():
        #     err = "Error: input file improperly formatted for {0}" \
        #           " (roles or nodes don't exist for this experiment)." \
        #           " Please check the config file" \
        #           " on fifeutilgpvm01. I will skip this experiment for now." \
        #           "\n".format(expt)
        #     error_handler(err)
        #     return False
        # return True

    # def check_output_mod(self, cmd):
    #     """A stripped-down version of subprocess.check_output in
    #     python 2.7+. Returns the stdout and return code"""
    #     process = subprocess.Popen(cmd, stdout=subprocess.PIPE,
    #                                stderr=subprocess.STDOUT, env=self.locenv)
    #     output, _ = process.communicate()
    #     retcode = process.poll()
    #     if retcode:
    #         raise Exception(output)
    #     return output, retcode

    def get_proxy(self, expt, voms_role, account):
        """Get the proxy for the role and experiment

        Returns proxy path (outfile) if proxy was successfully generated
        """
        voms_prefix = self.config['experiments'][expt]['vomsgroup'] \
            if 'vomsgroup' in self.config['experiments'][expt] \
            else 'fermilab:/fermilab/{0}/'.format(expt)
        voms_string = '{0}Role={1}'.format(voms_prefix, voms_role)

        certfile = self.config['experiments'][expt]['certfile'] \
            if 'certfile' in self.config['experiments'][expt] \
            else os.path.join(self.CERT_BASE_DIR, '{0}.cert'.format(account))
        keyfile = self.config['experiments'][expt]['keyfile'] \
            if 'keyfile' in self.config['experiments'][expt] \
            else os.path.join(self.CERT_BASE_DIR, '{0}.key'.format(account))

        outfile = '{0}.{1}.proxy'.format(account, voms_role)

        vpi_args = ["/usr/bin/voms-proxy-init", '-rfc', '-valid', '24:00', '-voms',
                    voms_string, '-cert', certfile,
                    '-key', keyfile, '-out',
                    'proxies/' + outfile]

        # do voms-proxy-init now
        try:
            self.check_output_mod(vpi_args)
        except Exception:
            err = "Error obtaining {0}.  Please check the cert on " \
                  "fifeutilgpvm01. " \
                  "Continuing on to next role.".format(outfile, self.CERT_BASE_DIR)
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
        account_node = '{0}@{1}.fnal.gov'.format(account, node)
        srcpath = os.path.join('proxies', outfile)
        newproxypath = os.path.join(self.config['experiments'][expt]["dir"], account, '{0}.new'.format(outfile))
        oldproxypath = os.path.join(self.config['experiments'][expt]["dir"], account, outfile)

        scp_cmd = ['scp', '-o', 'ConnectTimeout=30', srcpath, '{0}:{1}'.format(account_node, newproxypath)]
        chmod_cmd = ['ssh', '-o', 'ConnectTimeout=30', account_node,
                             'chmod 400 {0} ; mv -f {0} {1}'.format(newproxypath, oldproxypath)]
        
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

    def process_experiment(self):
        """Function to process each experiment, including sending the proxy onto its nodes"""
        # global expt_files
        print 'Now processing ' + self.expt
        # expt_file = "log_{0}".format(expt)
        # expt_files[expt] = expt_file

        # with expt_log_active(self.logger, expt, level=logging.WARN):
        badnodes = []
        expt_success = True

        # if not self.check_keys(expt): return False

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
                outfile = self.get_proxy(self.expt, role, acct)
            except Exception as e:
                # We couldn't get a proxy - so just move to the next role
                expt_success = False
                self.logger.error(e)
                # error_handler(e)
                continue

            # OK, we got a ticket and a proxy, so let's try to copy
            for node in nodes:
                try:
                    self.copy_proxy(node, acct, self.expt, outfile)
                except Exception as e:
                    self.logger.error(e)
                    # error_handler(e)
                    expt_success = False
                    if node in badnodes:
                        string = "Node {0} didn't respond to pings earlier - " \
                                    "so it's expected that copying there would fail.".format(node)
                        self.logger.warn(string)

        return expt_success


    # def process_all_experiments(self):
    #     """Main execution method to process all experiments"""
    #     successful_expts = (expt for expt in self.config['experiments'].iterkeys()
    #                         if self.process_experiment(expt))

    #     self.logger.info("This run completed successfully for the following "
    #                   "experiments: {0}.".format(', '.join(successful_expts)))


def main():
    """Main execution module"""
    # global logger, config
    successful_experiments = []
    failed_experiments = []
    second_try = {}

    args = parse_arguments()

    try:
        config = load_config(args.config, args.test)
    except Exception as e:
        err = 'Could not load config file.  Error is {0}'.format(e)
        print err
        # DO SOMETHING HERE
        # error_handler(err)
        sys.exit(1)

    # Main Log queue and listener process
    m = Manager()
    log_msg_queue = m.Queue()
    listener = Process(target=main_logger, args=(log_msg_queue, config))
    listener.start()

    # logger = setup_logger("Managed Proxy Push")
    if args.test: log_msg_queue.put(None, logging.INFO, "Running in test mode")
    log_msg_queue.put((None, logging.INFO, "Using config file {0}".format(args.config)))

    # Are we running as the right user?
    try:
        assert check_user(config['global']['should_runuser'])
    except AssertionError:
        log_msg_queue.put((None, logging.ERROR, "Script must be run as {0}. Exiting.".format(config['global']['should_runuser'])))
        kill_main_logger(log_msg_queue)
        exit(1)

    # Setup temp log dir
    try: listdir(temp_log_dir)
    except OSError: mkdir(temp_log_dir)

    # Get a kerb ticket
    try:
        kerb_ticket_obtain(config['global']['KRB5CCNAME'])
    except Exception as e:
        w = 'WARNING: Error obtaining kerberos ticket; ' \
                'may be unable to push proxies.  Error was {0}\n'.format(e)
        log_msg_queue.put(None, logging.WARN, w)
        # self.logger.warning(err)


    numworkers = 1 if args.experiment else 5
    expts = [expt, ] if args.experiment else config['experiments'].keys()
    # if args.experiment:
    #     numworkers = 1

    # Set up the worker pool
    pool = Pool(processes=numworkers)

    # Start workers, send jobs to the processes
    
    results = {}
    for expt in expts:
        results[expt] = pool.apply_async(run_worker, (expt, config, log_msg_queue))

    # If we ever upgrade to python 2.7...
    # results = {expt: pool.apply_async(run_worker, (expt, config, log_msg_queue))}

    pool.close()

    for expt, result in results.iteritems():
        try:
            successful_experiments.append(result.get(timeout=SOFT_TIMEOUT))
            cleanup_expt(expt, log_msg_queue, config)
        except TimeoutError:
            second_try[expt] = result
            log_msg_queue.put((expt, logging.DEBUG, "{0} hit the soft timeout.  Will try to get result in next round".format(expt)))
        except Exception as e:
            log_msg_queue.put((expt, logging.ERROR, e))

    for expt, result in second_try.iteritems():
        try:
            # You have numsec seconds to get the result.  if not, it doesn't go to the  successful experiments
            successful_experiments.append(result.get(timeout=HARD_TIMEOUT))
        except TimeoutError:
            msg = "{} hit the hard timeout.  Exiting process"
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
                log_msg_queue.put(
                    (None, logging.ERROR, "Could not cleanup {0}. Error was {1}".format(expt, e)))
                # sleep(10)
                # kill_main_logger(log_msg_queue)
                # log_msg_queue.put(None)
                # exit(1)

    pool.terminate()
    pool.join()

    log_msg_queue.put((None, logging.INFO,"Successful Experiments: {0}".format(successful_experiments)))
    log_msg_queue.put((None, logging.INFO, "Failed: {0}".format(failed_experiments)))

    
    




    # Verify general cleanup actions 
    # - send email to us, 
    # - send slack to us
    # - kill main logger
    # - listener.join()

    
    cleanup_global(config, log_msg_queue)
    listener.join()

    # log_msg_queue.put_nowait(None)  # Poison pill
    # sleep(0.1)

    # try:
    #     m = ManagedProxyPush(config, logger)
    # except Exception as e:
    #     error_handler(e)
    #     sys.exit(1)
    # else:   # We instantiated the ManagedProxyPush class, with all its checks
    #     try:
    #         # Now let's actually process the experiments
    #         if args.experiment:
    #             # If we've specified one experiment
    #             m.process_experiment(args.experiment)
    #             logger.info("Successfully pushed proxy for {0}".format(args.experiment))
    #         else:
    #             m.process_all_experiments()
    #     except Exception as e:
    #         error_handler(e)
    #         sys.exit(1)
    # finally:
    #     send_all_notifications(args.test)


if __name__ == '__main__':
    main()
    sys.exit(0)
