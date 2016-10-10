#!/usr/bin/env bash

if [ "$TRAVIS_TAG" = "" ] && [ "$TRAVIS_JDK_VERSION" = "oraclejdk8" ]; then
    mvn deploy -P shade,sources,javadoc,sign -Djava.version=1.6 --settings .travis/settings.xml
else
    mvn clean verify -P shade,sources,javadoc,sign -Djava.version=1.6 --settings .travis/settings.xml
fi
