package com.github.handrake.krocks

import com.github.handrake.krocks.IteratorExtensions.isValidKey
import com.github.handrake.krocks.IteratorExtensions.isValidPrefix
import com.github.handrake.krocks.TransactionDBExtensions.decr
import com.github.handrake.krocks.TransactionDBExtensions.del
import com.github.handrake.krocks.TransactionDBExtensions.incr
import com.github.handrake.krocks.TransactionDBExtensions.set
import com.github.handrake.krocks.TransactionDBExtensions.get
import org.rocksdb.RocksIterator
import org.rocksdb.Transaction
import org.rocksdb.WriteOptions


class KSetRocks(private val db: KRocksDB) {
    fun sadd(key: String, vararg members: String) {
        val writeOptions = WriteOptions()
        val transaction = db.underlying.beginTransaction(writeOptions)

        transaction.sadd(key, *members)

        transaction.commit()
    }

    fun Transaction.sadd(key: String, vararg members: String) {
        for (member in members) {
            if (!this.sismember(key, member)) {
                val setKey = buildSetKey(key, member)

                this.set(setKey, "1")
                this.incr(buildSetLenKey(key))
            }
        }
    }

    fun srem(key: String, member: String) {
        val writeOptions = WriteOptions()
        val transaction = db.underlying.beginTransaction(writeOptions)

        if (transaction.sismember(key, member)) {
            val setKey = buildSetKey(key, member)
            transaction.del(setKey)
            transaction.decr(buildSetLenKey(key))
        }

        transaction.commit()
    }

    fun sismember(key: String, member: String): Boolean {
        return db.get(buildSetKey(key, member)) != null
    }

    fun Transaction.sismember(key: String, member: String): Boolean {
        return this.get(buildSetKey(key, member)) != null
    }

    fun RocksIterator.sismember(key: String, member: String): Boolean {
        val setKey = buildSetKey(key, member)

        this.seek(buildSetKey(key, member).toByteArray(KRocksDB.CHARSET))

        return this.isValidKey(setKey)
    }

    fun scard(key: String): Long {
        return db.get(buildSetLenKey(key))?.toLongOrNull() ?: 0L
    }

    fun sdiff(vararg keys: String): Set<String> {
        if (keys.isEmpty()) {
            return emptySet()
        }

        val iter = db.underlying.newIterator()

        return iter.sdiff(*keys)
    }

    fun RocksIterator.sdiff(vararg keys: String): Set<String> {
        return keys.drop(1).fold(this.smembers(keys.first())) { acc, key ->
            acc - this.smembers(key)
        }
    }

    fun sdiffstore(destination: String, vararg keys: String): Int {
        val writeOptions = WriteOptions()
        val transaction = db.underlying.beginTransaction(writeOptions)

        val iter = db.underlying.newIterator()
        val diff = iter.sdiff(*keys).toTypedArray()

        transaction.sadd(destination, *diff)

        transaction.commit()

        return diff.size
    }

    fun sinter(vararg keys: String): Set<String> {
        if (keys.isEmpty()) {
            return emptySet()
        }

        val iter = db.underlying.newIterator()

        return keys.drop(1).fold(iter.smembers(keys.first())) { acc, key ->
            acc.intersect(iter.smembers(key))
        }
    }

    fun sunion(vararg keys: String): Set<String> {
        if (keys.isEmpty()) {
            return emptySet()
        }

        val iter = db.underlying.newIterator()

        return keys.drop(1).fold(iter.smembers(keys.first())) { acc, key ->
            acc.union(iter.smembers(key))
        }
    }

    fun RocksIterator.sunion(vararg keys: String): Set<String> {
        return keys.drop(1).fold(this.smembers(keys.first())) { acc, key ->
            acc.union(this.smembers(key))
        }
    }

    fun sunionstore(destination: String, vararg keys: String): Int {
        val writeOptions = WriteOptions()
        val transaction = db.underlying.beginTransaction(writeOptions)

        val iter = db.underlying.newIterator()
        val union = iter.sunion(*keys).toTypedArray()

        transaction.sadd(destination, *union)

        transaction.commit()

        return union.size
    }

    fun smembers(key: String): Set<String> {
        val iter = db.underlying.newIterator()

        return iter.smembers(key)
    }

    fun RocksIterator.smembers(key: String): Set<String> {
        val prefix = buildSetPrefix(key)

        this.seek(prefix.toByteArray(KRocksDB.CHARSET))

        val result = mutableSetOf<String>()

        while (this.isValidPrefix(prefix)) {
            result.add(getSetValue(this.key().toString(KRocksDB.CHARSET)))
            this.next()
        }

        return result
    }

    private fun setSetLen(key: String, len: Long) {
        db.set(buildSetLenKey(key), len.toString())
    }

    private fun buildSetPrefix(key: String): String {
        return "KRocks:Set:$key"
    }

    private fun getSetValue(keyWithPrefix: String): String {
        return removeSetPrefix(keyWithPrefix).split(":")[1]
    }

    private fun removeSetPrefix(keyWithPrefix: String): String {
        return keyWithPrefix.replace("$SET_PREFIX:", "")
    }

    private fun buildSetKey(key: String, member: String): String {
        return "$SET_PREFIX:$key:$member"
    }

    private fun buildSetLenKey(key: String): String {
        return "KRocks:Len:Set:$key"
    }

    companion object {
        private const val SET_PREFIX = "KRocks:Set"
    }
}