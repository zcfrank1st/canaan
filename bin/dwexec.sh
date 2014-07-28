#!/bin/sh

source /etc/profile
source ${HOME}/.bash_profile

export CANAAN_CONF_SUBDIR=dwdev
export CANAAN_HOME=/data/deploy/canaan/
export HIVE_HOME=$HIVE_INSTALL
#export HIVE_HOME=/usr/local/hadoop/hive-0.8.1/
export CLASSPATH=${CLASSPATH}:${CANAAN_HOME}/lib/
export ZIPPER_HOME=/data/deploy/dwarch/bin/bi_hadoop/zipper/
CANAAN_JAR_PATH=${CANAAN_HOME}/lib/canaan-0.2.0-SNAPSHOT.jar

java -jar ${CANAAN_JAR_PATH} "$@" 

#for param in $@; do
#echo $param
#done
