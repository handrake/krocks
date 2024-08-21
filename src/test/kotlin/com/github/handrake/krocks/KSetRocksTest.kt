package com.github.handrake.krocks

import org.junit.jupiter.api.AfterAll
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.BeforeAll
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.TestInstance

@TestInstance(TestInstance.Lifecycle.PER_CLASS)
internal class KSetRocksTest {
    private lateinit var store: KRocksDB
    private lateinit var kset: KSetRocks

    @BeforeAll
    fun setUp() {
        store = KRocksDB("./test-db")
        kset = KSetRocks(store)
    }

    @Test
    fun testSaddSrem() {
        val (key, member) = "1" to "2"

        kset.sadd(key, member)

        assertEquals(true, kset.sismember(key, member))

        kset.srem(key, member)

        assertEquals(false, kset.sismember(key, member))
    }

    @Test
    fun testScard() {
        for (i in 0 until 100) {
            kset.sadd("0", (i+1).toString())
        }

        assertEquals(100, kset.scard("0"))
    }

    @AfterAll
    fun tearDown() {
        store.close()
        store.destroy()
    }
}
