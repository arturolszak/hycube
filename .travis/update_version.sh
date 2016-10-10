#!/usr/bin/env bash

if [ "$TRAVIS_TAG" != "" ] ; then
    echo "This is a tagged build -> updating version to $TRAVIS_TAG"
    mvn versions:set -DnewVersion="$TRAVIS_TAG"
else
    echo "This is a snapshot build -> updating version to $TRAVIS_BRANCH-SNAPSHOT"
    mvn versions:set -DnewVersion="$TRAVIS_BRANCH-SNAPSHOT"
fi
