package com.github.handrake.krocks

interface KRocks {
    fun set(key: String, value: String)
    fun get(key: String): String?

    fun close()
    fun destroy()
}
