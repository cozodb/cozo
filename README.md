## Build

First build static lib for RocksDB

```bash
cd rocksdb
USE_RTTI=1 DEBUG_LEVEL=0 make static_lib
```

## Edge key layout

* Different src/tgt
  * Forward `[true, src_keys, null, tgt_keys]`
  * Backward `[false, src_keys, null, tgt_keys]`
* Same src/tgt
  * Forward `[null, src_keys, true, tgt_keys]`
  * Backward `[null, src_keys, false, tgt_keys]`

## Isolation levels

* Read uncommitted: write to the raw DB
* Read committed: use transaction
* Repeatable read: use snapshot
* Serializable: do all reads with `GetForUpdate`