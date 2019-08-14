#/bin/bash

echo "${GOPATH:?Need to have $GOPATH set}"

NAME=managed-proxies
GOIMPORTPATH=cdcvs.fnal.gov/discompsupp/ken_proxy_push
RPMARCH=i696-redhat-linux 

STARTDIR=${GOPATH}/src/${GOIMPORTPATH}
PACKAGEINFOFILE=${STARTDIR}/packaging/packaging.go
SPECFILE=${STARTDIR}/packaging/${NAME}.spec
VERSION=`grep "Version = " ${PACKAGEINFOFILE} | cut -d \" -f 2`
DIRNAME=${NAME}-${VERSION}
SOURCEDIR=${STARTDIR}/${DIRNAME}
DESTSOURCESDIR=${HOME}/rpmbuild/SOURCES
DESTSPECSDIR=${HOME}/rpmbuild/SPECS
PACKAGEFILES="${STARTDIR}/cmd/check-certs/check-certs ${STARTDIR}/cmd/proxy-push/proxy-push ${STARTDIR}/cmd/store-in-myproxy/store-in-myproxy ${STARTDIR}/managedProxies.yml ${STARTDIR}/packaging/${NAME}.cron ${STARTDIR}/packaging/${NAME}.logrotate"
BUILD=`date -u +%Y-%m-%dT%H:%M:%SZ`


# Rebuild executables
cd ${STARTDIR}/cmd
for dir in `ls -1` 
do
  echo ${dir}
  cd ${STARTDIR}/cmd/${dir}
  GOOS=linux go build -ldflags="-X packaging.Build=${BUILD}"

  if [[ $? -ne 0 ]] ; then
    echo "Could not build executable in directory $dir"
    exit 1
  fi

  echo "Built ${dir} executable"
done 


# Rewrite spec with proper version
TEMPFILE=`mktemp` || exit 1
while read line
do
	if [[ $line == Version* ]] ; then
	  echo "Version:        $VERSION"
  else
	  echo "$line"
  fi 
done < ${SPECFILE} > ${TEMPFILE}


if [[ $? == 0 ]] ; then
	mv ${TEMPFILE} ${SPECFILE}
	echo "Set version in spec file"	
else
	echo "Could not set version in temp spec file"
	rm ${TEMPFILE}
	exit 1
fi

# Create the RPM
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
