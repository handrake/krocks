package com.github.handrake.krocks

import com.github.handrake.krocks.ByteArrayExtensions.toB
import com.github.handrake.krocks.ByteArrayExtensions.toS
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

    fun Transaction.incr(key: String) {
        this.merge(key.toB(), (1L).toB())
    }

    fun Transaction.decr(key: String) {
        this.merge(key.toB(), (-1L).toB())
    }
}