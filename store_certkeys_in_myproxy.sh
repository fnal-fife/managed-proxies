#!/bin/bash

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

myproxy-init  -c 0 -s myproxy-int.fnal.gov -xZ '/DC=org/DC=opensciencegrid/O=Open Science Grid/OU=Services/CN=fifebatch-(preprod|dev).fnal.gov'  -t 24 -l "`openssl x509 -in $X509_USER_CERT -noout -subject | cut -d " " -f 2-`"

# store in production

myproxy-init  -c 0 -s myproxy.fnal.gov -xZ '/DC=org/DC=opensciencegrid/O=Open Science Grid/OU=Services/CN=(fifebatch|(hepcjobsub0(1|2))).fnal.gov'  -t 24 -l "`openssl x509 -in $X509_USER_CERT -noout -subject | cut -d " " -f 2-`"
