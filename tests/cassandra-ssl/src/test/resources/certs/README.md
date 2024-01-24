These long-lived certificates were created to test the SSL, they are in no way secure.

To create them, [I followed the instructions here](https://docs-previous.pega.com/decision-management/86/creating-java-keystores-and-truststores-cassandra-encryption):

Which are:

```
keytool -genkey -keyalg RSA -alias shared -validity 36500 \
    -keystore keystore.shared -storepass cassandra -keypass cassandra -dname \
    "CN=None, OU=None, O=None, L=None, C=None"
```

then:

```
keytool -export -alias shared -file shared.cer -keystore \
    keystore.shared -storepass cassandra
```

then:

```
keytool \
    -importcert -v -trustcacerts -noprompt -alias shared -file shared.cer \
    -keystore truststore.shared -storepass cassandra
```
