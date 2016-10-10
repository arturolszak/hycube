#!/usr/bin/env bash

if [ "$TRAVIS_TAG" = "" ] ; then

    mvn versions:set -DnewVersion="$TRAVIS_BRANCH-SNAPSHOT"
else
    mvn versions:set -DnewVersion="$TRAVIS_TAG"
fi
