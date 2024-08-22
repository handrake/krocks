package com.github.handrake.krocks

import com.github.handrake.krocks.StringExtensions.toB
import com.github.handrake.krocks.StringExtensions.toS
import org.rocksdb.ReadOptions
import org.rocksdb.Transaction

object TransactionDBExtensions {
    fun Transaction.set(key: String, value: String) {
        this.put(key.toB(), value.toB())
    }

    fun Transaction.get(key: String): String? {
        return runCatching {
            val readOptions = ReadOptions()
            return this.get(readOptions, key.toB()).toS()
        }.getOrNull()
    }

    fun Transaction.del(key: String) {
        this.delete(key.toB())
    }

    fun Transaction.incr(key: String): Long {
        val newValue = (this.get(key)?.toLongOrNull() ?: 0L) + 1
        this.set(key, newValue.toString())
        return newValue
    }

    fun Transaction.decr(key: String): Long {
        val newValue = (this.get(key)?.toLongOrNull() ?: 0L) - 1
        this.set(key, newValue.toString())
        return newValue
    }
}