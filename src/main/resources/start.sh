#!/bin/bash

HOME=`pwd`
PID_FILE="/tmp/rkr-cacheservice.pid"

HEAP="-Xmx6g"

CONF_FILE="-DconfigFile=${HOME}/application.conf"
#LOG4J_FILE="-Dlog4j.configuration=${HOME}/log4j.properties"

JAR_FILE=`ls ${HOME}/cacheService*dependencies.jar`

if [[ ! -f ${JAR_FILE} ]];then
   echo ${JAR_FILE}
   echo "Jar file does not exist"
   exit
fi

java ${CONF_FILE} ${HEAP} -jar ${JAR_FILE}

echo $! > $PID_FILE
