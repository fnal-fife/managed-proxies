#!/usr/bin/env python
import shlex
import subprocess
import json
import sys
import smtplib
from os import environ, devnull, geteuid
from pwd import getpwuid
from datetime import datetime




#Functions

def sendemail(errorstring,errorfile):
    """Function to send email after error string is populated.
       Only argument is errorstring, which is a formatted string of the errors generated in this script.
    """
    sender = 'fife-group@fnal.gov'
    receivers = 'fife-group@fnal.gov'    # Change to fife-group@fnal.gov when ready for production
    messageheader = """From:  FIFEUTILGPVM01 <fife-group.fnal.gov>
To:  FIFE-GROUP <fife-group@fnal.gov>
Subject:  proxy_push.py errors

"""
    message = messageheader + errorstring

    try:
        smtpObj = smtplib.SMTP('smtp.fnal.gov')
        smtpObj.sendmail(sender,receivers,message)
        print "Successfully sent email"
    except Exception as e:
        err = "Error:  unable to send email.\n%s\n" % e
        print err
        with open(errorfile,'a') as f:
            f.write("\n%s"% err)
        raise 
    return None


def errout(error,errorstring,errorfile):
    """Function to handle how errors are logged and displayed.   We write error to the errorfile, and then add error to the errorstring, which gets returned.
    Arguments:
        error (string):  The current error being logged
        errorstring (string):  The string of all errors encountered in this script's execution so far
        errorfile (string): The file we're writing the errors to
    """
    with open(errorfile,'a') as f:
        if len(errorstring) == 0:
            f.write("\n%s" % datetime.now())
        f.write("\n%s"% error)
    print error
    errorstring+=error
    return errorstring



#Script

# Error handling variables.  
errfile = 'proxy_push.err'      #Set the output file for errors.  Times in errorfile are local time.
allerrstr = ''                  #error string that will get passed into the email when populated


#Displays who is running this script.  Will not allow running as root
should_runuser= 'rexbatch'

runuser= getpwuid(geteuid())[0]
print "Running script as %s." % runuser

if runuser <> should_runuser:      #uid = 47535 is rexbatch
    err = "This script must be run as %s. Exiting." % should_runuser
    allerrstr = errout(err,allerrstr,errfile)    
    raise OSError(err)

#grab the initial environment
locenv=environ.copy()
locenv['KRB5CCNAME']="FILE:/tmp/krb5cc_push_proxy"

# base location for certs/keys
CERT_BASE_DIR="/opt/gen_push_proxy/certs"


# Obtain a ticket based on the special use principal
try :
    kerbcmd = [ "/usr/krb5/bin/kinit",'-k','-t','/opt/gen_keytabs/gcso_monitor_rexbatch.keytab','monitor/gcso/fermigrid.fnal.gov@FNAL.GOV']
    krb5init = subprocess.Popen(kerbcmd,env=locenv)
except :
    err='Error obtaining kerberos ticket; unable to push proxy' 
    allerrstr = errout(err,allerrstr,errfile)
    sendemail(allerrstr,errfile)
    sys.exit(1)

proxylist=open('input_file.json','r')

#make proxy for each role and push to each machine

myjson=json.load(proxylist)

for expt in myjson.keys():
    print('Now processing ' + expt)
    if "roles" not in myjson[expt].keys() or "nodes" not in myjson[expt].keys():
        err="Error: input file improperly formatted for %s (roles or nodes don't exist for this experiment). Please check ~rexbatch/gen_push_proxy/input_file.json on fifeutilgpvm01. I will skip this experiment for now.\n" % expt
        allerrstr = errout(err,allerrstr,errfile)
        continue
    nodes=myjson[expt]["nodes"]
    for role in myjson[expt]["roles"] :
        voms_role = role.keys()[0]
        account=role[voms_role]
#        print voms_role, account
        voms_string='fermilab:/fermilab/' + expt + '/Role=' + voms_role
#        print(voms_string)
        outfile=account + '.' + voms_role + '.proxy'
#        print(outfile)
        vpi_args=["/usr/bin/voms-proxy-init", '-rfc', '-valid', '24:00', '-voms', voms_string, '-cert' , CERT_BASE_DIR + '/' + account + '.cert', '-key', CERT_BASE_DIR + '/' + account + '.key', '-out', 'proxies/' + outfile ]
#        print vpi_args
        # do voms-proxy-init now
        try:
            vpi=subprocess.Popen(vpi_args,stdout=subprocess.PIPE,stderr=subprocess.PIPE)
        except:
            err = "Error obtaining %s.  Please check the cert in %s on fifeutilgpvm01. Continuing on to next role.\n" % (outfile,CERT_BASE_DIR)
            allerrstr = errout(err,allerrstr,errfile)
            continue
        # now try to get the proper kerberos ticket
        # Set the name of the temp file to generate

        # OK, we got a ticket and a proxy, so let's try to copy
        for node in nodes :
            # first we check the .k5login file to see if we're even allowed to push the proxy
            k5login_check = 'ssh ' + account + '@' + node + ' cat .k5login'
            nNames=-1
#            try :
#                k5check=subprocess.Popen(k5login_check,stdout=subprocess.PIPE,env=locenv)
#                wcminusl=subprocess.Popen( ['wc','-l'],stdin=k5check.stdout)
#                k5check.stdout.close()
#                nNames=wcminusl.communicate()[0]
#            except :
#                print("Error checking .k5login file for account %s. Not pushing the proxy to this node." % account)
#                continue
#            finally:
#                wcminusl.close()
            dest=account + '@' + node + ':' + myjson[expt]["dir"] + '/' + account + '/' + outfile
            scp_cmd = [ 'scp','proxies/'+outfile, dest ]
	    try :
                #proxypush=subprocess.Popen(scp_cmd,stdout=subprocess.PIPE,env=locenv)
                with open(devnull,'w') as f:
		    subprocess.check_call(scp_cmd,stdout=f,env=locenv)
            except subprocess.CalledProcessError as e:
                err = "Error copying ../proxies/%s to %s. Trying next node\n %s" % (outfile, node,str(e))
                allerrstr = errout(err,allerrstr,errfile)
                continue

if len(allerrstr) > 0:
    sendemail(allerrstr,errfile)

