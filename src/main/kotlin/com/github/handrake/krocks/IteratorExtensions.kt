package com.github.handrake.krocks

import com.github.handrake.krocks.StringExtensions.toS
import org.rocksdb.RocksIterator

object IteratorExtensions {
    fun RocksIterator.isValidPrefix(prefix: String): Boolean {
        return this.isValid && this.key().toS().startsWith(prefix)
    }

    fun RocksIterator.isValidKey(key: String): Boolean {
        return this.isValid && this.key().toS() == key
    }
}
