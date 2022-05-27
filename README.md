## Build

First build static lib for RocksDB

```bash
cd rocksdb
USE_RTTI=1 DEBUG_LEVEL=0 make static_lib
```

## Edge key layout

* Forward `[true, *src_keys, *tgt_keys, *own_keys]`
* Backward `[false, *src_keys, *tgt_keys, *own_keys]`

## Isolation levels

* Read uncommitted: write to the raw DB
* Read committed: use transaction
* Repeatable read: use snapshot
* Serializable: do all reads with `GetForUpdate`