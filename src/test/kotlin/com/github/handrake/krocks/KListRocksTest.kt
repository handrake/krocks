package com.github.handrake.krocks

import org.junit.jupiter.api.AfterEach
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.Test

internal class KListRocksTest {
    private lateinit var store: KRocksDB
    private lateinit var klist: KListRocks

    @BeforeEach
    fun setUp() {
        store = KRocksDB("./test-db")
        klist = KListRocks(store)
    }

    @Test
    fun testPushPop() {
        klist.rpush("1", "2")
        klist.rpush("1", "3")
        klist.lpush("1", "4")

        assertEquals(3L, klist.llen("1"))

        val result = mutableListOf<String>()

        result.add(klist.lpop("1").first())
        result.add(klist.rpop("1").first())
        result.add(klist.rpop("1").first())

        assertEquals(listOf("4", "3", "2"), result)
        assertEquals(emptyList<String>(), klist.rpop("1"))
    }

    @AfterEach
    fun tearDown() {
        store.close()
        store.destroy()
    }
}