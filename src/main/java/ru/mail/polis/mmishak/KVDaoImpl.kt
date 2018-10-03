package ru.mail.polis.mmishak

import ru.mail.polis.KVDao
import java.io.File
import java.io.IOException
import java.nio.file.Files
import java.util.*
import javax.xml.bind.DatatypeConverter

class KVDaoImpl(private val storageDirectory: File) : KVDao {

    @Throws(NoSuchElementException::class, IOException::class)
    override fun get(key: ByteArray): ByteArray {
        val file = getFile(key)
        if (!file.exists()) throw NoSuchElementException("File with key'" + String(key) + "' not exits")
        return Files.readAllBytes(file.toPath())
    }

    @Throws(IOException::class)
    override fun upsert(key: ByteArray, value: ByteArray) {
        val file = getFile(key)
        if (!file.exists()) {
            if (!file.createNewFile()) throw IOException()
        }
        Files.write(file.toPath(), value)
    }

    @Throws(IOException::class)
    override fun remove(key: ByteArray) {
        val file = getFile(key)
        val success = !file.exists() || file.delete()
        if (!success) throw IOException()
    }

    override fun close() {
    }

    private fun getFile(key: ByteArray): File {
        return File(storageDirectory.absolutePath + SEPARATOR + keyToString(key))
    }

    private fun keyToString(key: ByteArray): String {
        return DatatypeConverter.printHexBinary(key)
    }

    companion object {
        private const val SEPARATOR = "/"
    }
}
