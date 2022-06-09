

Encrypt

 - standalone

```
time java -jar target/cloudproof-demo-2.0.0.jar --encrypt \
    -k src/test/resources/keys/public_key.json \
    -o "hdfso://root@localhost:9000/user/root/" \
    src/test/resources/users.txt
```

 - spark

 ```sh
 time ./spark-run-local.sh --encrypt \
    -k src/test/resources/keys/public_key.json \
    -o "hdfso://root@localhost:9000/user/root/" \
    src/test/resources/users.txt
```



Search All FR

- standalone


```sh
time java -jar target/cloudproof-demo-2.0.0.jar --search\
    -k src/test/resources/keys/user_SuperAdmin_key.json \
    -o src/test/resources/dec/ \
    -c search_FR_ALL.txt \
    "hdfso://root@localhost:9000/user/root/" \
    "country=France"
```

- spark

```sh
time ./spark-run-local.sh --search\
    -k src/test/resources/keys/user_SuperAdmin_key.json \
    -o src/test/resources/dec/ \
    -c search_FR_ALL.txt \
    "hdfso://root@localhost:9000/user/root/" \
    "country=France"
```


Search DE

```sh
time java -jar target/cloudproof-demo-2.0.0.jar --search\
    -k src/test/resources/keys/user_SuperAdmin_key.json \
    -o src/test/resources/dec/ \
    -c search_DE_ALL.txt \
    "hdfso://root@localhost:9000/user/root/" \
    "country=DE"
```

...which cannot be decrypted with the BNL Italy key

```sh
time java -jar target/cloudproof-demo-2.0.0.jar --search\
    -k src/test/resources/keys/user_BNL_Italy_key.json \
    -o src/test/resources/dec/ \
    -c search_DE_ALL.txt \
    "hdfso://root@localhost:9000/user/root/" \
    "country=DE"
```

The search defaults to conjunction (AND)

```sh
time java -jar target/cloudproof-demo-2.0.0.jar --search\
    -k src/test/resources/keys/user_SuperAdmin_key.json \
    -o src/test/resources/dec/ \
    -c search_DE_SOLA.txt \
    "hdfso://root@localhost:9000/user/root/" \
    "country=DE" "party=SOLA"
```

... and this search does not return any result

```sh
time java -jar target/cloudproof-demo-2.0.0.jar --search\
    -k src/test/resources/keys/user_SuperAdmin_key.json \
    -o src/test/resources/dec/ \
    -c search_DE_NL.txt \
    "hdfso://root@localhost:9000/user/root/" \
    "country=DE" "country=NL"
```

... however one can switch to disjunction (OR) using `--or`

```sh
time java -jar target/cloudproof-demo-2.0.0.jar --search --or\
    -k src/test/resources/keys/user_SuperAdmin_key.json \
    -o src/test/resources/dec/ \
    -c search_DE_NL.txt \
    "hdfso://root@localhost:9000/user/root/" \
    "country=DE" "country=NL"
```

