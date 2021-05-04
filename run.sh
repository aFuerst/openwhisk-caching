#!/bin/sh
# run with sudo
export JAVA_HOME="/home/alfuerst/jre1.8.0_261"
rm -f ./bin/*.jar
# rm -rf $(find ./core -name build)
./gradlew :core:standalone:build
# --api-gw --api-gw-port 4569 
sudo env "PATH=/home/alfuerst/repos/wsk-cli" /home/alfuerst/jre1.8.0_261/bin/java -jar ./bin/openwhisk-standalone.jar --dev-mode --ui-port 7894  -c ./bin/application.conf