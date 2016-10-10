#!/usr/bin/env bash

echo "Securely removing key files..."
shred -n 100 -z -u .travis/keys/pubring.gpg
shred -n 100 -z -u .travis/keys/secring.gpg
