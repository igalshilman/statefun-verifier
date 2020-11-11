### StateFun Runtime Verification Job

```
docker-compose build
docker-compose up --scale worker=2 --abort-on-container-exit  --exit-code-from master
```

The expected return code should be 0.

