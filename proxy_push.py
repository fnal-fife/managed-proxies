#!/usr/bin/env python
import subprocess
import json
import sys
import smtplib
import logging
from os import environ, devnull, geteuid, remove
from os.path import exists
from pwd import getpwuid
from datetime import datetime
import smtplib
import email.utils
from email.mime.text import MIMEText


# Global Variables

# JSON input file
inputfile = 'input_file.json'

# Logging/Error handling variables.
logfile = 'proxy_push.log'
errfile = 'proxy_push.err'      # Set the output file for errors.  Times in errorfile are local time.
# allerrstring = ''                  # error string that will get passed into the email when populated

expt_error = False
numerrors = 0
logger = None

# Displays who is running this script.  Will not allow running as root
should_runuser = 'rexbatch'

# grab the initial environment
locenv = environ.copy()
locenv['KRB5CCNAME'] = "FILE:/tmp/krb5cc_push_proxy"

# base location for certs/keys
CERT_BASE_DIR = "/opt/gen_push_proxy/certs"

# Functions

def setupLogger(scriptname):
    global logfile, errfile
    # Create Logger
    logger = logging.getLogger(scriptname)
    logger.setLevel(logging.DEBUG)

    # Console handler - info
    ch = logging.StreamHandler()
    ch.setLevel(logging.INFO)

    # Logfile Handler
    lh = logging.FileHandler(logfile)
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

# def sendemail():
#     """Function to send email after error string is populated.
#        Only argument is errorstring, which is a formatted string of
#        the errors generated in this script.
#     """
#     global errfile
#
#     sender = 'fife-group@fnal.gov'
#     receivers = 'fife-group@fnal.gov'    # Change to fife-group@fnal.gov when ready for production
#     messageheader = """From:  FIFEUTILGPVM01 <fife-group.fnal.gov>
# To:  FIFE-GROUP <fife-group@fnal.gov>
# Subject:  proxy_push.py errors
#
# """
#     message = messageheader + allerrstring
#
#     try:
#         smtpObj = smtplib.SMTP('smtp.fnal.gov')
#         smtpObj.sendmail(sender, receivers, message)
#         print "Successfully sent email"
#     except Exception as e:
#         err = "Error:  unable to send email.\n%s\n" % e
#         print err
#         with open(errfile, 'a') as f:
#             f.write("\n%s" % err)
#         raise
#     return None


def sendemail():
    """Function to send email after message string is populated."""
    global logger

    with open(errfile, 'r') as f:
        message = f.read()

    sender = 'fife-group@fnal.gov'
    msg = MIMEText(message)
    msg['To'] = email.utils.formataddr(('FIFE GROUP', 'fife-group@fnal.gov'))
    msg['From'] = email.utils.formataddr(('FIFEUTILGPVM01', sender))
    msg['Subject'] = "proxy_push.py errors"

    try:
        smtpObj = smtplib.SMTP('smtp.fnal.gov')
        smtpObj.sendmail(sender, [sender], msg.as_string())
        # logger.info(message)
        logger.info("Successfully sent error email")
    except Exception as e:
        err = "Error:  unable to send email.\n%s\n" % e
        logger.error(err)
        raise
    return



# def errout(error):
#     """Function to handle how errors are logged and displayed.   We write error to the errorfile, and then add error to the errorstring, which gets returned.
#     Arguments:
#         error (string):  The current error being logged
#         errorstring (string):  The string of all errors encountered in this script's execution so far
#         errorfile (string): The file we're writing the errors to
#     """
#     global errfile
#     global allerrstring
#
#     with open(errfile, 'a') as f:
#         if len(allerrstring) == 0:
#             f.write("\n\n%s" % datetime.now())
#         f.write("\n%s" % error)
#     print error
#     allerrstring += error
#     return None


def check_user(authuser):
    """Check to see who's running script.  If not rexbatch, exit"""
    runuser = getpwuid(geteuid())[0]
    print runuser
    print "Running script as {0}.".format(runuser)
    return runuser == authuser


def kerb_ticket_obtain():
    """Obtain a ticket based on the special use principal"""
    global locenv, logger
    try:
        kerbcmd = ['/usr/krb5/bin/kinit', '-k', '-t',
                   '/opt/gen_keytabs/config/gcso_monitor.keytab',
                   'monitor/gcso/fermigrid.fnal.gov@FNAL.GOV']
        krb5init = subprocess.check_call(kerbcmd, env = locenv)
    except:
        err = 'WARNING: Error obtaining kerberos ticket; ' \
              'may be unable to push proxies'
        logger.warning(err)
        # sendemail()
    return None


def loadjson(infile):
    with open(infile, 'r') as proxylist:
        myjson = json.load(proxylist)
    return myjson


def check_keys(expt, myjson):
    """Make sure our JSON file has nodes and roles for the experiment"""
    global logger
    if "roles" not in myjson[expt].keys() or "nodes" not in myjson[expt].keys():
        err = "Error: input file improperly formatted for {0}" \
              " (roles or nodes don't exist for this experiment)." \
              " Please check ~rexbatch/gen_push_proxy/input_file.json" \
              " on fifeutilgpvm01. I will skip this experiment for now." \
              "\n".format(expt)
        logger.error(err)
        return False
    return True


def get_proxy(role, expt):
    """Get the proxy for the role and experiment"""
    global CERT_BASE_DIR, logger, expt_error
    voms_role = role.keys()[0]
    account = role[voms_role]
    voms_string = 'fermilab:/fermilab/' + expt + '/Role=' + voms_role

    if expt == "des":
        voms_string = expt +':/' + expt + '/Role=' + voms_role

    outfile = account + '.' + voms_role + '.proxy'
    vpi_args = ["/usr/bin/voms-proxy-init", '-rfc', '-valid', '24:00', '-voms',
                voms_string, '-cert' , CERT_BASE_DIR + '/' + account + '.cert',
                '-key', CERT_BASE_DIR + '/' + account + '.key', '-out',
                'proxies/' + outfile]

    # do voms-proxy-init now
    try:
        vpi = subprocess.Popen(vpi_args, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
    except:
        err = "Error obtaining {0}.  Please check the cert in {1} on " \
              "fifeutilgpvm01. " \
              "Continuing on to next role.".format(outfile, CERT_BASE_DIR)
        logger.error(err)
        return False, account
    return outfile, account


def copy_proxy(node, account, myjson, expt, outfile):
    """Copies the proxies to submit nodes"""
    global locenv, logger

    # first we check the .k5login file to see if we're even allowed to push the proxy
    k5login_check = 'ssh ' + account + '@' + node + ' cat .k5login'
    nNames = -1

    dest = account + '@' + node + ':' + myjson[expt]["dir"] + '/' + account + '/' + outfile
    newproxy = myjson[expt]["dir"] + '/' + account + '/' + outfile + '.new'
    oldproxy = myjson[expt]["dir"] + '/' + account + '/' + outfile
    chmod_cmd = ['ssh', '-ak', account + '@' + node,
                 'chmod 400 {0} ; mv -f {1} {2}'.format(newproxy, newproxy, oldproxy)]
    scp_cmd = ['scp', 'proxies/' + outfile, dest + '.new']

    try:
        with open(devnull, 'w') as f:
            subprocess.check_call(scp_cmd, stdout=f, env=locenv)
            try:
                subprocess.check_call(chmod_cmd, stdout=f, env=locenv)
            except subprocess.CalledProcessError as e:
                err = "Error changing permission of {0} to mode 400 on {1}. " \
                      "Trying next node\n {2}".format(outfile, node, str(e))
                logger.exception(err)
                return False
    except subprocess.CalledProcessError as e:
        err = "Error copying ../proxies/{0} to {1}. " \
              "Trying next node\n {2}".format(outfile, node, str(e))
        logger.exception(err)
        return False
    return True


def process_experiment(expt, myjson):
    """Function to process each experiment, including sending the proxy onto its nodes"""
    global expt_success, numerrors
    print 'Now processing ' + expt
    expt_error = False

    numerrors = 0

    if not check_keys(expt, myjson):
        expt_success = False
        numerrors += 1
        return False

    nodes = myjson[expt]["nodes"]

    for role in myjson[expt]["roles"] :
        outfile, account = get_proxy(role, expt)

        if not outfile:
            numerrors += 1
            expt_success = True
            # return False

        # OK, we got a ticket and a proxy, so let's try to copy
        for node in nodes :
            if not copy_proxy(node, account, myjson, expt, outfile):
                numerrors += 1

    return numerrors


def main():
    """Main execution module"""
    global should_runuser, inputfile, logger

    logger = setupLogger("Managed Proxy Push")

    if not check_user(should_runuser):
        err = "This script must be run as {0}. Exiting.".format(should_runuser)
        logger.error(err)
        raise OSError(err)

    kerb_ticket_obtain()
    myjson = loadjson(inputfile)


    successful_expts = []
    for expt in myjson.keys():
        process_experiment(expt, myjson)
        if not numerrors:
            successful_expts.append(expt)

    logger.info("This run completed successfully for the following "
                "experiments: {0}.".format(', '.join(successful_expts)))

    if exists(errfile):
        sendemail()
        remove(errfile)

    return


if __name__ == '__main__':
    try:
        main()
        sys.exit(0)
    except:
        sys.exit(1)
