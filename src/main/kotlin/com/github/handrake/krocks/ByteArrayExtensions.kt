package com.github.handrake.krocks

import java.nio.ByteBuffer
import java.nio.ByteOrder

object ByteArrayExtensions {
    val CHARSET = Charsets.UTF_8

    fun String.toB(): ByteArray = this.toByteArray(CHARSET)
    fun ByteArray.toS(): String = this.toString(CHARSET)

    fun Long.toB(): ByteArray {
        val buffer = ByteBuffer.allocate(Long.SIZE_BYTES).order(ByteOrder.LITTLE_ENDIAN)
        buffer.putLong(this)
        return buffer.array()
    }

    fun ByteArray.toL(): Long {
        val buffer = ByteBuffer.allocate(Long.SIZE_BYTES).order(ByteOrder.LITTLE_ENDIAN)
        buffer.put(this)
        buffer.flip()
        return buffer.getLong()
    }
}
