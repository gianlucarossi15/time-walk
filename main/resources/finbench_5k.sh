#!/bin/bash

cd "$NEO4J_HOME/bin" || exit 1
./cypher-shell -u neo4j -p reds2000 -f ../import/sf0.01/snapshot/finBenchIngestion.cypher