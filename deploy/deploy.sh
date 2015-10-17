#!/bin/bash

VERSION=2.1.1

pushd . > /dev/null
cd ..
mkdir -p ./deploy/Cygnus/conf
cp -r ./compiler ./deploy/Cygnus/
cp -r ./protobuf ./deploy/Cygnus/
cp -r ./install.sh ./deploy/Cygnus/
cp -r ./README.txt ./deploy/Cygnus/
cp -r ./DSL.txt ./deploy/Cygnus/
cp -r ./conf/silk.conf ./deploy/Cygnus/conf/sampleSilk.conf
cp -r ./conf/palo-alto-access.conf ./deploy/Cygnus/conf/samplePaloAltoAccessLogs.conf
popd > /dev/null
tar --exclude=".svn" -czf Cygnus-$VERSION.tar.gz ./Cygnus
rm -rf ./Cygnus

