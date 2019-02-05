#/bin/bash

echo "${GOPATH:?Need to have $GOPATH set}"

GOIMPORTPATH=cdcvs.fnal.gov/discompsupp/ken_proxy_push
RPMARCH=i696-redhat-linux 

STARTDIR=${GOPATH}/src/${GOIMPORTPATH}
NAME=managed-proxies
VERSION=`grep Version ${STARTDIR}/packaging/${NAME}.spec | awk '{print $2}'`
DIRNAME=${NAME}-${VERSION}
SOURCEDIR=${STARTDIR}/${DIRNAME}
DESTSOURCESDIR=${HOME}/rpmbuild/SOURCES
DESTSPECSDIR=${HOME}/rpmbuild/SPECS
PACKAGEFILES="${STARTDIR}/cmd/check-certs/check-certs ${STARTDIR}/cmd/proxy-push/proxy-push ${STARTDIR}/cmd/store-in-myproxy/store-in-myproxy ${STARTDIR}/managedProxies.yml ${STARTDIR}/packaging/${NAME}.cron ${STARTDIR}/packaging/${NAME}.logrotate"


cd $STARTDIR

if  [ -d "$SOURCEDIR" ]; then 
    rm -Rf $SOURCEDIR
fi

# Create directory to be tarred
mkdir -p $SOURCEDIR
for FILE in $PACKAGEFILES; do
    cp $FILE ${SOURCEDIR}/
done

# Templates
cp -r ${STARTDIR}/templates ${SOURCEDIR}/templates

# Tar it up
tar -cvzf ${DESTSOURCESDIR}/${DIRNAME}.tar.gz ${SOURCEDIR##*/}

# Copy the spec file into place
cp ${PWD}/packaging/${NAME}.spec ${DESTSPECSDIR}/

# Build our RPM
cd $DESTSPECSDIR
rpmbuild -ba --target $RPMARCH ${NAME}.spec

# Clean up
cd $STARTDIR
rm -Rf $SOURCEDIR

exit $?
