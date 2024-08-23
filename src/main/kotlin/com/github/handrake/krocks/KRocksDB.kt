package com.github.handrake.krocks

import com.github.handrake.krocks.ByteArrayExtensions.toB
import com.github.handrake.krocks.ByteArrayExtensions.toS
import org.rocksdb.OptimisticTransactionDB
import org.rocksdb.Options
import org.rocksdb.RocksDB
import org.rocksdb.UInt64AddOperator

class KRocksDB(private val path: String) {
    val underlying: OptimisticTransactionDB
    private val options: Options = Options()

    init {
        this.options.setCreateIfMissing(true)
        this.options.setCreateMissingColumnFamilies(true)
        this.options.setMergeOperatorName("uint64add")
        this.options.setMergeOperator(UInt64AddOperator())

        this.underlying = OptimisticTransactionDB.open(options, path)
    }

    fun set(key: String, value: String) {
        underlying.put(key.toB(), value.toB())
    }

    fun get(key: String): String? {
        return runCatching {
            underlying.get(key.toB()).toS()
        }.getOrNull()
    }

    fun del(key: String) {
        underlying.delete(key.toB())
    }

    fun incr(key: String) {
        underlying.merge(key.toB(), (1L).toB())
    }

    fun decr(key: String) {
        underlying.merge(key.toB(), (-1L).toB())
    }

    fun close() {
        underlying.close()
    }

    fun destroy() {
        RocksDB.destroyDB(path, options)
    }
}
