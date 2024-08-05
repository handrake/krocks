package com.github.handrake.krocks

import org.rocksdb.Options
import org.rocksdb.RocksDB

@Suppress("JoinDeclarationAndAssignment")
class KRocksImpl : KRocks {
    private val db: RocksDB
    private val path: String
    private val options: Options

    constructor(path: String) {
        this.options = Options()
        this.options.setCreateIfMissing(true)
        this.options.setCreateMissingColumnFamilies(true)

        this.db = RocksDB.open(options, path)
        this.path = path
    }

    override fun set(key: String, value: String) {
        db.put(key.toByteArray(CHARSET), value.toByteArray(CHARSET))
    }

    override fun get(key: String): String? {
        return runCatching {
            db.get(key.toByteArray(CHARSET)).toString(CHARSET)
        }.getOrNull()
    }

    override fun sadd(key: String, member: String) {
        db.put(buildSetKey(key, member).toByteArray(CHARSET), "1".toByteArray(CHARSET))
    }

    override fun srem(key: String, member: String) {
        db.delete(buildSetKey(key, member).toByteArray(CHARSET))
    }

    override fun sismember(key: String, member: String): Boolean {
        return db.get(buildSetKey(key, member).toByteArray(CHARSET)) != null
    }

    override fun close() {
        db.close()
    }

    override fun destroy() {
        RocksDB.destroyDB(path, options)
    }

    private fun buildSetKey(key: String, member: String): String {
        return "KRocks:Set:$key:$member"
    }

    companion object {
        private val CHARSET = Charsets.UTF_8
    }
}
