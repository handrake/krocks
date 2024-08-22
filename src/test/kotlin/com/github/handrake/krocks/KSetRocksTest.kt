package com.github.handrake.krocks

import org.junit.jupiter.api.AfterEach
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.Test

internal class KSetRocksTest {
    private lateinit var store: KRocksDB
    private lateinit var kset: KSetRocks

    @BeforeEach
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

    @Test
    fun testSUnion() {
        for (i in 0 until 10) {
            kset.sadd("0", i.toString())
        }

        for (i in 10 until 20) {
            kset.sadd("1", i.toString())
        }

        for (i in 20..30) {
            kset.sadd("2", i.toString())
        }

        val result = kset.sunion("0", "1", "2").map { it.toInt() }.toSet()

        assertEquals((0..30).toSet(), result)
    }

    @AfterEach
    fun tearDown() {
        store.close()
        store.destroy()
    }
}
