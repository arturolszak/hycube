#!/usr/bin/env bash

echo "Securely removing key files..."
shred -n 100 -z -u .travis/pubring.gpg
shred -n 100 -z -u .travis/secring.gpg
