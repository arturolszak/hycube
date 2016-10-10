#!/usr/bin/env bash

echo "Decrypting keys for signing artifacts..."
openssl aes-256-cbc -pass pass:$ENCRYPTION_PASSWORD -in .travis/secring.gpg.enc -out .travis/secring.gpg -d
openssl aes-256-cbc -pass pass:$ENCRYPTION_PASSWORD -in .travis/pubring.gpg.enc -out .travis/pubring.gpg -d