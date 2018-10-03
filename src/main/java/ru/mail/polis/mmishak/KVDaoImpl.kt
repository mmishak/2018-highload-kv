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
        return when {
            file.exists() -> file.toByteArray()
            else -> throw NoSuchElementException()
        }
    }

    @Throws(IOException::class)
    override fun upsert(key: ByteArray, value: ByteArray) {
        val file = getFile(key)
        when {
            file.exists() -> file.write(value)
            file.createNewFile() -> file.write(value)
            else -> throw IOException()
        }
    }

    @Throws(IOException::class)
    override fun remove(key: ByteArray) {
        val file = getFile(key)
        val success = file.deleteIfExists()
        if (!success) throw IOException()
    }

    override fun close() {
    }

    private fun getFile(key: ByteArray) =
        File(storageDirectory.absolutePath + SEPARATOR + key.hexString)

    private fun File.toByteArray() = Files.readAllBytes(this.toPath())

    private fun File.write(value: ByteArray) = Files.write(this.toPath(), value)

    private fun File.deleteIfExists() = if (exists()) delete() else true

    private val ByteArray.hexString: String
        get() = DatatypeConverter.printHexBinary(this)

    companion object {
        private const val SEPARATOR = "/"
    }
}
