package com.exactpro.th2.common.utils

fun interface CheckedConsumer<T> {
    @Throws(Throwable::class) fun consume(value: T)
}