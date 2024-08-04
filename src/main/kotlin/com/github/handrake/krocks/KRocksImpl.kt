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

    override fun close() {
        db.close()
    }

    override fun destroy() {
        RocksDB.destroyDB(path, options)
    }

    companion object {
        private val CHARSET = Charsets.UTF_8
    }
}
