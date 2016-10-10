#!/usr/bin/env bash

echo "Decrypting keys for signing artifacts..."
openssl aes-256-cbc -pass pass:$ENCRYPTION_PASSWORD -in .travis/keys/secring.gpg.enc -out .travis/keys/secring.gpg -d
openssl aes-256-cbc -pass pass:$ENCRYPTION_PASSWORD -in .travis/keys/pubring.gpg.enc -out .travis/keys/pubring.gpg -d
