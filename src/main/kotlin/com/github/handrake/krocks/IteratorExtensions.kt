package com.github.handrake.krocks

import org.rocksdb.RocksIterator

object IteratorExtensions {
    fun RocksIterator.isValidPrefix(prefix: String): Boolean {
        return this.isValid && this.key().toString(KRocksDB.CHARSET).startsWith(prefix)
    }

    fun RocksIterator.isValidKey(key: String): Boolean {
        return this.isValid && this.key().toString(KRocksDB.CHARSET) == key
    }
}
