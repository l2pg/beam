#!/bin/bash
GLOBALBLOCK=/mnt/resource/blockall
BLOCKNAME=/mnt/resource/block_moremmr_pipeline_1
LOG_FILE=/mnt/resource/moremmr_pipeline_1_log_`date +"%Y%m%d_%H%M%S"`.txt

if [ -f $GLOBALBLOCK ] ; then
    echo `date +"%Y-%m-%d %H:%M:%S"`" All jobs execution is blocked by blockall"
    exit
fi

if ! [ -f $BLOCKNAME ] ; then
    touch $BLOCKNAME

    echo "Launched moremmr_pipeline_1..."
      
    /usr/beam_moremmr/gradlew :beam-sdks-python:itemsMeanPurchaseTime -PjobEndpoint=localhost:8099 >> $LOG_FILE 2>&1

    echo "Finished moremmr_pipeline_1"

    rm $BLOCKNAME
    exit
fi
if [ -f $BLOCKNAME ] ; then
    echo `date +"%Y-%m-%d %H:%M:%S"`" Block file detected"
    exit
fi