#!/bin/bash

wget -k 'https://s3.amazonaws.com/geotrellis-test/geowave-minicluster/miniAccumuloCluster-assembly-0.1.0.jar'

java -jar miniAccumuloCluster-assembly-0.1.0.jar
