#!/bin/bash

rm ./*.jar
cd ..

rm -r ~/.m2/repository/org/apache/giraph/
cd jars/ || exit
./install.sh
cd ../

mvn clean package
cp target/WICM-1.0-SNAPSHOT-jar-with-dependencies.jar build/
rm -r target

cd build || exit
