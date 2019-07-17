#!/bin/bash
./gradlew clean distTar
rm -r bin/jar
mkdir -p bin/jar && tar -C bin/jar/ -xf kcbq-confluent/build/distributions/kcbq-confluent-*.tar