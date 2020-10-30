#!/usr/bin/env bash

# Export your host here to launch the program on the platform
# export HOST=

export USER=root
export DEST_DIR="/home/root/publish_to_atlas"

echo "Create needed directory on platform and send required files there"

ssh ${USER}@${HOST} "mkdir -p ${DEST_DIR}/"
ssh ${USER}@${HOST} "mkdir -p ${DEST_DIR}/resources/"

scp src/main/resources/log4j2.properties ${USER}@${HOST}:${DEST_DIR}/
scp src/main/resources/application.conf ${USER}@${HOST}:${DEST_DIR}/
scp src/main/resources/launch.sh ${USER}@${HOST}:${DEST_DIR}/

ssh ${USER}@${HOST} "chmod +x ${DEST_DIR}/launch.sh"

scp target/publish-to-atlas-*-jar*.jar ${USER}@${HOST}:${DEST_DIR}/publish-to-atlas.jar

echo "Finished to send required files"

echo "Launch script on platform to launch program properly"
ssh ${USER}@${HOST} 'bash -s' < src/main/resources/launch.sh $@
echo "Program finished"


