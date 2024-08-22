package com.github.handrake.krocks

object StringExtensions {
    val CHARSET = Charsets.UTF_8

    fun String.toB(): ByteArray = this.toByteArray(CHARSET)
    fun ByteArray.toS(): String = this.toString(CHARSET)
}
