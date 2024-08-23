package com.github.handrake.krocks

import com.github.handrake.krocks.ByteArrayExtensions.toB
import org.junit.jupiter.api.AfterEach
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.Test
import org.rocksdb.WriteOptions

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
        assertEquals(1L, kset.scard(key))

        kset.srem(key, member)

        assertEquals(false, kset.sismember(key, member))
        assertEquals(0L, kset.scard(key))
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

    @Test
    fun testSDiff() {
        kset.sadd("s1", "1", "2", "3", "4")
        kset.sadd("s2", "2", "3", "5")

        assertEquals(setOf("1", "4"), kset.sdiff("s1", "s2"))
    }

    @Test
    fun testSInter() {
        kset.sadd("s1", "1", "2", "3", "4")
        kset.sadd("s2", "2", "3", "5")

        assertEquals(setOf("2", "3"), kset.sinter("s1", "s2"))
    }

    @Test
    fun testSMembers() {
        kset.sadd("s1", "1", "2", "3")
        kset.sadd("s1", "-1", "-2", "-3")

        assertEquals(setOf("-3", "-2", "-1", "1", "2", "3"), kset.smembers("s1"))
    }

    @AfterEach
    fun tearDown() {
        store.close()
        store.destroy()
    }
}
