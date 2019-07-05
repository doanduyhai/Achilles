#!/usr/bin/env bash

cd ../achilles-model && \
mvn clean install && \
cd ../achilles-common && \
mvn clean install && \
cd ../achilles-core