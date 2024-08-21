package com.github.handrake.krocks

import org.rocksdb.Options
import org.rocksdb.RocksDB

class KRocksDB(private val path: String) {
    val underlying: RocksDB
    private val options: Options = Options()

    init {
        this.options.setCreateIfMissing(true)
        this.options.setCreateMissingColumnFamilies(true)
        this.underlying = RocksDB.open(options, path)
    }

    fun set(key: String, value: String) {
        underlying.put(key.toByteArray(CHARSET), value.toByteArray(CHARSET))
    }

    fun get(key: String): String? {
        return runCatching {
            underlying.get(key.toByteArray(CHARSET)).toString(CHARSET)
        }.getOrNull()
    }

    fun del(key: String) {
        underlying.delete(key.toByteArray(CHARSET))
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

    companion object {
        val CHARSET = Charsets.UTF_8
    }
}
