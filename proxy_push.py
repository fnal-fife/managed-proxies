#!/usr/bin/env python
import shlex
import subprocess
import json
from os import environ, devnull, geteuid
import sys
from pwd import getpwuid
from datetime import datetime

# Set the output file for errors.  Times in errorfile are local time.
errfile = 'proxy_push.err'


#Displays who is running this script.  Will not allow running as root
print "Running script as %s." % getpwuid(geteuid())[0]

if geteuid() <> 47535:      #uid = 47535 is rexbatch
    err = "This script must be run as rexbatch. Exiting."
    with open(errfile,"a") as f:
        f.write("\n%s\n%s\n"%(datetime.now(),err))
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
    print('Error obtaining kerberos ticket for %s; unable to push proxy' )
    sys.exit(1)

proxylist=open('input_file.json','r')

#make proxy for each role and push to each machine

myjson=json.load(proxylist)

for expt in myjson.keys():
    print('Now processing ' + expt)
    if "roles" not in myjson[expt].keys() or "nodes" not in myjson[expt].keys():
        print("Error: input file improperly formatted for %s. Please check it. I will skip this experiment for now." % expt)
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
            print("Error obtaining %s. Continuing on to next role.\n" % outfile)
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
                with open(errfile,"a") as f:
                    f.write("\n%s\n%s\n"%(datetime.now(),err))
                print err
                continue
