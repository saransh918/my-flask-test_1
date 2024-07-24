#!/bin/bash
gunicorn --bind=0.0.0.0:8000 flask_sample:app
wget https://download.java.net/java/GA/jdk11/9/GPL/openjdk-11_linux-x64_bin.tar.gz
tar -xzf openjdk-11_linux-x64_bin.tar.gz
export JAVA_HOME=$(pwd)/jdk-11
export HADOOP_HOME=$(pwd)/Hadoop