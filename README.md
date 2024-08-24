# KRocks: Simple redis-like embedded key-value store

Currently, Redis String, List, and Set are implemented for the following commands. All commands are atomic operations using [RocksDB OptimisticTransactionDB](https://github.com/facebook/rocksdb/wiki/Transactions).

## String
- GET
- SET
- DEL

## List
- EXISTS
- LLEN
- LINDEX
- LPUSH
- RPUSH
- LPOP
- RPOP

## Set
- SADD
- SREM
- SISMEMBER
- SCARD
- SDIFF
- SDIFFSTORE
- SINTER
- SUNION
- SUNIONSTORE
- SMEMBERS

All data is persisted on disk through RocksDB.

