#!/bin/sh
# run with sudo
rm -f ./bin/*.jar
rm -rf $(find ./core -name build)
./gradlew :core:standalone:build
# --api-gw --api-gw-port 4569 
export CONFIG_whisk_limits_actionCodeSize="100 m"
export CONFIG_whisk_limits_parameterSize="100 m"
sudo env "PATH=/home/alfuerst/repos/wsk-cli" /usr/bin/java -jar ./bin/openwhisk-standalone.jar --dev-mode --ui-port 7894 -c ./bin/application.conf