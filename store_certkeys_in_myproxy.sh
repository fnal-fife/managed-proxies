#!/bin/bash
#set -x
ACCT=$1

if [ $# -lt 1 ]; then
echo "Error, you must specify the account for which you want to store the cert and key."
exit 1
fi

BASEDIR=/opt/gen_push_proxy/certs
CERTFILE=${BASEDIR}/${ACCT}.cert
KEYFILE=${BASEDIR}/${ACCT}.key

if [ ! -f $CERTFILE ]; then
echo "Error, $CERTFILE not found"
exit 1
fi

if [ ! -f $KEYFILE ]; then
echo "Error, $KEYFILE not found"
exit 1
fi

export X509_USER_CERT=$CERTFILE
export X509_USER_KEY=$KEYFILE

# store in myproxy-int for the preprod and dev servers
default_ppretrievers="/DC=org/DC=opensciencegrid/O=Open Science Grid/OU=Services/CN=(fifebatch-(preprod|dev)|htcjsdev0(1|2)|jobsub-dev).fnal.gov"
cigetcertopts=`curl -f -s --capath /etc/grid-security/certificates https://fifebatch-dev.fnal.gov/cigetcertopts.txt`
if [ $? -eq 0 ]; then
    ppretrievers=`echo $cigetcertopts | grep -o -E -e "--myproxyretrievers=('.*')" | sed -e "s/--myproxyretrievers=//" -e "s/'//g"`
    if [ "$ppretrievers" != "$default_ppretrievers" ]; then 
	echo "ATTENTION: The dev and preprod retrievers option, ${ppretrievers}, has changed from the default option. Contact HTC to see if this is a permanent change. If so, consider changing the default option in this script."
    fi
    myproxy-init -c 0 -s myproxy-int.fnal.gov -xZ "${ppretrievers}" -t 24 -l "`openssl x509 -in $X509_USER_CERT -noout -subject | cut -d " " -f 2-`" || echo "Error when storing proxy in myproxy-int."
else
    echo "Error reading cigetcertopts.txt. Using default value."
    ppretrievers=$default_ppretrievers
fi

# read cigetcertopts from the production fifebatch server, then store in production

default_retrievers="/DC=org/DC=opensciencegrid/O=Open Science Grid/OU=Services/CN=(fifebatch|(hepcjobsub0(1|2))).fnal.gov"
cigetcertopts=`curl -f -s --capath /etc/grid-security/certificates  https://fifebatch.fnal.gov/cigetcertopts.txt`
RESULT=$?
if [ $RESULT -eq 0 ]; then
    retrievers=`echo $cigetcertopts | grep -o -E -e "--myproxyretrievers=('.*')" | sed -e "s/--myproxyretrievers=//" -e "s/'//g"`
    if [ "$retrievers" != "$default_retrievers" ]; then
        echo "ATTENTION: The production retrievers option, ${retrievers}, has changed from the default option. Contact HTC to see if this is a permanent change. If so, consider changing the default option in this script."
    fi
    myproxy-init  -c 0 -s myproxy.fnal.gov -xZ "${retrievers}" -t 24 -l "`openssl x509 -in $X509_USER_CERT -noout -subject | cut -d " " -f 2-`" || echo "Error when storing proxy in myproxy."
else
    echo "Error reading cigetcertopts.txt. Using default value."
    retrievers=$default_retrievers
fi



