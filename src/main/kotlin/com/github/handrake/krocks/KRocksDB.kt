package com.github.handrake.krocks

import com.github.handrake.krocks.StringExtensions.toB
import com.github.handrake.krocks.StringExtensions.toS
import org.rocksdb.OptimisticTransactionDB
import org.rocksdb.Options
import org.rocksdb.ReadOptions
import org.rocksdb.RocksDB
import org.rocksdb.Transaction

class KRocksDB(private val path: String) {
    val underlying: OptimisticTransactionDB
    private val options: Options = Options()

    init {
        this.options.setCreateIfMissing(true)
        this.options.setCreateMissingColumnFamilies(true)

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

    fun incr(key: String): Long {
        val newValue = (get(key)?.toLongOrNull() ?: 0L) + 1
        set(key, newValue.toString())
        return newValue
    }

    fun decr(key: String): Long {
        val newValue = (get(key)?.toLongOrNull() ?: 0L) - 1
        set(key, newValue.toString())
        return newValue
    }

    fun close() {
        underlying.close()
    }

    fun destroy() {
        RocksDB.destroyDB(path, options)
    }
}
