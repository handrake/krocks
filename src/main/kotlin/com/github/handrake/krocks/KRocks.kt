package com.github.handrake.krocks

interface KRocks {
    fun set(key: String, value: String)
    fun get(key: String): String?

    fun sadd(key: String, member: String)
    fun srem(key: String, member: String)
    fun sismember(key: String, member: String): Boolean

    fun close()
    fun destroy()
}
