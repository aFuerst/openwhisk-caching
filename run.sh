#!/bin/sh
# run with sudo
rm -f ./bin/openwhisk-standalone.jar
./gradlew :core:standalone:build
# --api-gw --api-gw-port 4569 
sudo env "PATH=/home/alfuerst/repos/wsk-cli" /usr/bin/java -jar ./bin/openwhisk-standalone.jar --dev-mode --ui-port 7894 -c ./bin/custom.conf