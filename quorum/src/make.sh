#!/bin/bash

thrift --gen java Address.thrift
thrift --gen java NodeService.thrift
thrift --gen java CoordinatorService.thrift
rm Address.java
rm NodeService.java
rm CoordinatorService.java
cp gen-java/Address.java ./
cp gen-java/NodeService.java ./
cp gen-java/CoordinatorService.java ./
rm -r gen-java

javac -cp ".:/usr/local/Thrift/*" *.java -d .
