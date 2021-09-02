#!/usr/bin/env bash

export DIR="/root/publish_to_atlas"

echo "*** Starting to launch program ***"

    cd $DIR

echo "Launching jar via java command"

    java -jar publish-to-atlas.jar $@

    sleep 1

echo "*** Finished program ***"