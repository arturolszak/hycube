#!/usr/bin/env bash

shred -n 100 -z -u .travis/pubring.gpg
shred -n 100 -z -u .travis/secring.gpg
