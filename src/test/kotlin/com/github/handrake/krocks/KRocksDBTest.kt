package com.github.handrake.krocks

import org.junit.jupiter.api.AfterAll
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.BeforeAll
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.TestInstance

@TestInstance(TestInstance.Lifecycle.PER_CLASS)
internal class KRocksDBTest {
    private lateinit var store: KRocksDB

    @BeforeAll
    fun setUp() {
        store = KRocksDB("./test-db")
    }

    @Test
    fun getset() {
        val (k, v) = "1" to "2"
        store.set(k, v)
        assertEquals(v, store.get(k))
    }

    @Test
    fun getsetKorean() {
        val (k, v) = "하나" to "둘"
        store.set(k, v)
        assertEquals(v, store.get(k))
    }

    @AfterAll
    fun tearDown() {
        store.close()
        store.destroy()
    }
}
