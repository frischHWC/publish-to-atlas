#!/usr/bin/env bash

export DIR="/home/root/scalaappcdpmaven"

echo "*** Starting to launch program ***"

    cd $DIR

echo "Launching jar via java command"

    java -jar scalaappcdpmaven.jar $@

    sleep 1

echo "*** Finished program ***"