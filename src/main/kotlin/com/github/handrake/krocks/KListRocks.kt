package com.github.handrake.krocks

import com.github.handrake.krocks.TransactionDBExtensions.set
import org.rocksdb.RocksIterator
import org.rocksdb.WriteOptions

class KListRocks(private val db: KRocksDB) {
    fun exists(key: String): Boolean {
        val iter = db.underlying.newIterator()
        val listKey = buildListPrefix(key)

        iter.seek(listKey.toByteArray(KRocksDB.CHARSET))

        return iter.isValid && iter.key().toString(KRocksDB.CHARSET).startsWith(listKey)
    }

    fun llen(key: String): Long {
        return db.underlying.newIterator().llen(key)
    }

    fun RocksIterator.llen(key: String): Long {
        val headIndex = this.getHeadIndex(key) ?: return 0L
        val tailIndex = this.getTailIndex(key) ?: return 0L

        return tailIndex - headIndex + 1
    }

    fun lpush(key: String, vararg elements: String) {
        push(key, Direction.LEFT, *elements)
    }

    fun rpush(key: String, vararg elements: String) {
        push(key, Direction.RIGHT, *elements)
    }

    private fun push(key: String, direction: Direction, vararg elements: String) {
        val writeOptions = WriteOptions()
        val transaction = db.underlying.beginTransaction(writeOptions)
        val iter = transaction.iterator

        val index = when (direction) {
            Direction.LEFT -> iter.getHeadIndex(key) ?: (INITIAL_INDEX + 1)
            Direction.RIGHT -> iter.getTailIndex(key) ?: (INITIAL_INDEX - 1)
        }

        for ((i, element) in elements.withIndex()) {
            val newIndex = when (direction) {
                Direction.LEFT -> index - i - 1
                Direction.RIGHT -> index + i + 1
            }
            val listKey = buildListKeyIndex(key, newIndex)
            transaction.set(listKey, element)
        }

        transaction.commit()
    }

    fun lpop(key: String, count: Long = 1): List<String> {
        return pop(key, Direction.LEFT, count)
    }

    fun rpop(key: String, count: Long = 1): List<String> {
        return pop(key, Direction.RIGHT, count)
    }

    private fun pop(key: String, direction: Direction, count: Long = 1): List<String> {
        val writeOptions = WriteOptions()
        val transaction = db.underlying.beginTransaction(writeOptions)
        val iter = transaction.iterator

        val index = iter.getEndIndex(key, direction) ?: return emptyList()

        val listKey = buildListKeyIndex(key, index)
        val listPrefix = buildListPrefix(key)

        iter.seek(listKey.toByteArray(KRocksDB.CHARSET))

        val result = mutableListOf<String>()

        for (i in 0 until count) {
            if (iter.isValid && iter.key().toString(KRocksDB.CHARSET).startsWith(listPrefix)) {
                result.add(iter.value().toString(KRocksDB.CHARSET))
                db.underlying.delete(iter.key())
                if (direction == Direction.LEFT) {
                    iter.next()
                } else {
                    iter.prev()
                }
            } else {
                break
            }
        }

        return result
    }

    private fun RocksIterator.getEndIndex(key: String, direction: Direction): Long? {
        return when (direction) {
            Direction.LEFT -> this.getHeadIndex(key)
            Direction.RIGHT -> this.getTailIndex(key)
        }
    }

    private fun RocksIterator.getHeadIndex(key: String): Long? {
        val index = buildHeadIndexKey(key)
        this.seek(index.toByteArray(KRocksDB.CHARSET))
        return this.getIndex(key)
    }

    private fun RocksIterator.getTailIndex(key: String): Long? {
        val index = buildTailIndexKey(key)
        this.seekForPrev(index.toByteArray(KRocksDB.CHARSET))
        return this.getIndex(key)
    }

    private fun RocksIterator.getIndex(key: String): Long? {
        return if (this.isValid && this.key().toString(KRocksDB.CHARSET).startsWith(buildListPrefix(key))) {
            getIndexFromPrefix(this.key().toString(KRocksDB.CHARSET))
        } else {
            null
        }
    }

    private fun buildListPrefix(key: String): String {
        return "$LIST_PREFIX:$key"
    }

    private fun buildListKeyIndex(key: String, index: Long): String {
        val indexHex = "%08x".format(index)
        return "${buildListPrefix(key)}:${indexHex}"
    }

    private fun buildHeadIndexKey(key: String): String {
        return "$LIST_PREFIX:$key:00000000"
    }

    private fun buildTailIndexKey(key: String): String {
        return "$LIST_PREFIX:$key:ffffffff"
    }

    private fun getIndexFromPrefix(keyWithPrefix: String): Long {
        return keyWithPrefix.split(":")[3].toLong(16)
    }

    enum class Direction {
        LEFT, RIGHT
    }

    companion object {
        private const val LIST_PREFIX = "KRocks:List"
        private const val INITIAL_INDEX = 2147483647L
    }
}