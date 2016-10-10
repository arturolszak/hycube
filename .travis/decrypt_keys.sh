#!/usr/bin/env bash

echo "Decrypting keys for signing artifacts..."
openssl aes-256-cbc -pass pass:$ENCRYPTION_PASSWORD -in .travis/secring.gpg.enc -out .travis/secring.gpg -d
openssl aes-256-cbc -pass pass:$ENCRYPTION_PASSWORD -in .travis/pubring.gpg.enc -out .travis/pubring.gpg -d
#openssl aes-256-cbc -K $encrypted_b9ed87107f17_key -iv $encrypted_b9ed87107f17_iv -in pubring.gpg.enc -out pubring.gpg -d
#openssl aes-256-cbc -K $encrypted_b9ed87107f17_key -iv $encrypted_b9ed87107f17_iv -in secring.gpg.enc -out secring.gpg -d
