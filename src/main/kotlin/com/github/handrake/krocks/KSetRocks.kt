package com.github.handrake.krocks

import com.github.handrake.krocks.IteratorExtensions.isValidKey
import com.github.handrake.krocks.IteratorExtensions.isValidPrefix
import com.github.handrake.krocks.ByteArrayExtensions.toB
import com.github.handrake.krocks.ByteArrayExtensions.toL
import com.github.handrake.krocks.ByteArrayExtensions.toS
import com.github.handrake.krocks.TransactionDBExtensions.decr
import com.github.handrake.krocks.TransactionDBExtensions.del
import com.github.handrake.krocks.TransactionDBExtensions.get
import com.github.handrake.krocks.TransactionDBExtensions.incr
import com.github.handrake.krocks.TransactionDBExtensions.set
import org.rocksdb.RocksIterator
import org.rocksdb.Transaction
import org.rocksdb.WriteOptions


class KSetRocks(private val db: KRocksDB) {
    fun sadd(key: String, vararg members: String) {
        val writeOptions = WriteOptions()
        val transaction = db.underlying.beginTransaction(writeOptions)

        sadd(transaction, key, *members)

        transaction.commit()
    }

    fun sadd(transaction: Transaction, key: String, vararg members: String) {
        for (member in members) {
            if (!sismember(transaction, key, member)) {
                val setKey = buildSetKey(key, member)

                transaction.set(setKey, "1")
                transaction.incr(buildSetLenKey(key))
            }
        }
    }

    fun srem(key: String, member: String) {
        val writeOptions = WriteOptions()
        val transaction = db.underlying.beginTransaction(writeOptions)

        if (sismember(transaction, key, member)) {
            val setKey = buildSetKey(key, member)
            transaction.del(setKey)
            transaction.decr(buildSetLenKey(key))
        }

        transaction.commit()
    }

    fun sismember(key: String, member: String): Boolean {
        return db.get(buildSetKey(key, member)) != null
    }

    fun sismember(transaction: Transaction, key: String, member: String): Boolean {
        return transaction.get(buildSetKey(key, member)) != null
    }

    fun sismember(iter: RocksIterator, key: String, member: String): Boolean {
        val setKey = buildSetKey(key, member)

        iter.seek(buildSetKey(key, member).toB())

        return iter.isValidKey(setKey)
    }

    fun scard(key: String): Long {
        return runCatching {
            db.underlying.get(buildSetLenKey(key).toB())
        }.getOrNull()?.toL() ?: 0L
    }

    fun sdiff(vararg keys: String): Set<String> {
        if (keys.isEmpty()) {
            return emptySet()
        }

        val iter = db.underlying.newIterator()

        return sdiff(iter, *keys)
    }

    fun sdiff(iter: RocksIterator, vararg keys: String): Set<String> {
        return keys.drop(1).fold(smembers(iter, keys.first())) { acc, key ->
            acc - smembers(iter, key)
        }
    }

    fun sdiffstore(destination: String, vararg keys: String): Int {
        val writeOptions = WriteOptions()
        val transaction = db.underlying.beginTransaction(writeOptions)

        val iter = db.underlying.newIterator()
        val diff = sdiff(iter, *keys).toTypedArray()

        sadd(transaction, destination, *diff)

        transaction.commit()

        return diff.size
    }

    fun sinter(vararg keys: String): Set<String> {
        if (keys.isEmpty()) {
            return emptySet()
        }

        val iter = db.underlying.newIterator()

        return keys.drop(1).fold(smembers(iter, keys.first())) { acc, key ->
            acc.intersect(smembers(iter, key))
        }
    }

    fun sunion(vararg keys: String): Set<String> {
        if (keys.isEmpty()) {
            return emptySet()
        }

        val iter = db.underlying.newIterator()

        return keys.drop(1).fold(smembers(iter, keys.first())) { acc, key ->
            acc.union(smembers(iter, key))
        }
    }

    fun sunion(iter: RocksIterator, vararg keys: String): Set<String> {
        return keys.drop(1).fold(smembers(iter, keys.first())) { acc, key ->
            acc.union(smembers(iter, key))
        }
    }

    fun sunionstore(destination: String, vararg keys: String): Int {
        val writeOptions = WriteOptions()
        val transaction = db.underlying.beginTransaction(writeOptions)

        val iter = db.underlying.newIterator()
        val union = sunion(iter, *keys).toTypedArray()

        sadd(transaction, destination, *union)

        transaction.commit()

        return union.size
    }

    fun smembers(key: String): Set<String> {
        val iter = db.underlying.newIterator()

        return smembers(iter, key)
    }

    fun smembers(iter: RocksIterator, key: String): Set<String> {
        val prefix = buildSetPrefix(key)

        iter.seek(prefix.toB())

        val result = mutableSetOf<String>()

        while (iter.isValidPrefix(prefix)) {
            result.add(getSetValue(iter.key().toS()))
            iter.next()
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