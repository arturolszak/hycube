The Travis build requires the following env variables:

SONATYPE_USERNAME
    the Sonatype user name for releasing to Maven Central Repository

SONATYPE_PASSWORD
    the Sonatype password for releasing to Maven Central Repository

ENCRYPTION_PASSWORD
    encryption password for decrypting public and private keys (used to sign artifacts)

GPG_PASSPHRASE
    private key passphrase (for signing artifacts)

The env variables can be stored in an encrypted form in Travis or directly in the .travis.yml file.
