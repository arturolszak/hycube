#!/usr/bin/env bash

if [ "$TRAVIS_TAG" != "" ] && [ "$TRAVIS_JDK_VERSION" = "oraclejdk8" ]; then
    echo "This is a tagged build with oraclejdk8 -> building and deploying..."
    mvn clean deploy -P sources,javadoc,sign -Djava.version=1.6 --settings .travis/settings.xml
else
    echo "This is NOT a tagged build with oraclejdk8 -> building only..."
    mvn clean verify -P sources,javadoc,sign -Djava.version=1.6 --settings .travis/settings.xml
fi
