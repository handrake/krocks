# KRocks: Simple redis-like embedded key-value store implemented on RocksDB

Currently, Redis String, List, and Set are implemented for the following commands.

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

