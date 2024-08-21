package com.github.handrake.krocks


class KSetRocks(private val db: KRocksDB) {
    fun sadd(key: String, vararg members: String) {
        for (member in members) {
            if (!sismember(key, member)) {
                val setKey = buildSetKey(key, member)

                db.set(setKey, "1")
                db.incr(buildSetLenKey(key))
            }
        }
    }

    fun srem(key: String, member: String) {
        if (sismember(key, member)) {
            val setKey = buildSetKey(key, member)
            db.del(setKey)
            db.decr(buildSetLenKey(key))
        }
    }

    fun sismember(key: String, member: String): Boolean {
        return db.get(buildSetKey(key, member)) != null
    }

    fun scard(key: String): Long {
        return db.get(buildSetLenKey(key))?.toLongOrNull() ?: 0L
    }

    fun sdiff(vararg keys: String): Set<String> {
        if (keys.isEmpty()) {
            return emptySet()
        }

        return keys.drop(1).fold(smembers(keys.first())) { acc, key ->
            acc - smembers(key)
        }
    }

    fun sdiffstore(destination: String, vararg keys: String): Int {
        val diff = sdiff(*keys).toTypedArray()

        sadd(destination, *diff)

        return diff.size
    }

    fun sinter(vararg keys: String): Set<String> {
        if (keys.isEmpty()) {
            return emptySet()
        }

        return keys.drop(1).fold(smembers(keys.first())) { acc, key ->
            acc.intersect(smembers(key))
        }
    }

    fun sunion(vararg keys: String): Set<String> {
        if (keys.isEmpty()) {
            return emptySet()
        }

        return keys.drop(1).fold(smembers(keys.first())) { acc, key ->
            acc.union(smembers(key))
        }
    }

    fun sunionstore(destination: String, vararg keys: String): Int {
        val union = sunion(*keys).toTypedArray()

        sadd(destination, *union)

        return union.size
    }

    fun smembers(key: String): Set<String> {
        val prefix = buildSetPrefix(key)

        val iter = db.underlying.newIterator()
        iter.seek(prefix.toByteArray(KRocksDB.CHARSET))

        val result = mutableSetOf<String>()

        while (iter.isValid && iter.key().toString(KRocksDB.CHARSET).startsWith(prefix)) {
            result.add(getSetValue(iter.key().toString(KRocksDB.CHARSET)))
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