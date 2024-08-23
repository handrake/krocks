package com.github.handrake.krocks

import com.github.handrake.krocks.ByteArrayExtensions.toB
import com.github.handrake.krocks.ByteArrayExtensions.toS
import org.rocksdb.RocksIterator

object IteratorExtensions {
    fun RocksIterator.isValidPrefix(prefix: String): Boolean {
        return this.isValid && this.key().toS().startsWith(prefix)
    }

    fun RocksIterator.isValidKey(key: String): Boolean {
        return this.isValid && this.key().toS() == key
    }

    fun RocksIterator.get(key: String): String {
        this.seek(key.toB())

        return if (this.isValidKey(key)) {
            this.value().toS()
        } else ""
    }
}
