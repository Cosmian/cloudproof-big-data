#!/bin/sh

sudo docker exec cloudproof-java-demo_hadoop-2.7.5_1 /bin/bash -c "/usr/local/hadoop-2.7.5/bin/hadoop fs $1 $2 $3"
