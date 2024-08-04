package com.github.handrake.krocks

import org.junit.jupiter.api.AfterAll
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.BeforeAll
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.TestInstance

@TestInstance(TestInstance.Lifecycle.PER_CLASS)
internal class KRocksImplTest {
    private lateinit var store: KRocks

    @BeforeAll
    fun setUp() {
        store = KRocksImpl("./test-db")
    }

    @Test
    fun getset() {
        val (k, v) = "1" to "2"
        store.set(k, v)
        assertEquals(v, store.get(k))
    }

    @AfterAll
    fun tearDown() {
        store.close()
        store.destroy()
    }
}
