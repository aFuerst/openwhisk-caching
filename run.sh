#!/bin/sh
# run with sudo
rm -f ./bin/openwhisk-standalone.jar
./gradlew :core:standalone:build
sudo env "PATH=$PATH" java -jar ./bin/openwhisk-standalone.jar --api-gw --api-gw-port 4569 --dev-mode --ui-port 7894 -c ./bin/custom.conf