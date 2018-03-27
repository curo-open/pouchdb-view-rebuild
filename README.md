# pouchdb-view-rebuild

## Test

```
export DB_SRC_PATH=<path to pouchdb with views inserted as _design documents>
export DB_RESULT_ROOT=/tmp/pdb-db-test

mocha --ui bdd  --timeout 100000
```