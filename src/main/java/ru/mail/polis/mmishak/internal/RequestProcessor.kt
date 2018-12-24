package ru.mail.polis.mmishak.internal

import ru.mail.polis.mmishak.internal.request.InternalRequest

interface RequestProcessor<T> {
    fun process(requests: List<InternalRequest<T>>): List<T>
}