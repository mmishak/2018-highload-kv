package ru.mail.polis.mmishak

import java.io.Serializable

sealed class Data : Serializable

class ExistsData(val time: Long, val value: ByteArray) : Data()

class DeletedData(val time: Long) : Data()

object AbsentData : Data()

object ErrorData : Data()
