package ru.mail.polis.mmishak

import org.rocksdb.Options
import org.rocksdb.RocksDB
import org.rocksdb.RocksDBException
import ru.mail.polis.KVDao
import java.io.File
import java.io.IOException
import java.nio.ByteBuffer
import java.util.*

class KVDaoImpl(storageDirectory: File) : KVDao {

    private var database: RocksDB = try {
        RocksDB.open(
            Options().setCreateIfMissing(true),
            storageDirectory.path
        )
    } catch (e: RocksDBException) {
        throw IllegalStateException(e)
    }

    @Throws(NoSuchElementException::class, IOException::class)
    override fun get(key: ByteArray) = try {
        database.get(key).ifExists()
    } catch (e: RocksDBException) {
        throw IOException(e)
    }

    @Throws(IOException::class)
    override fun upsert(key: ByteArray, value: ByteArray) = try {
        database.put(key, value.toStoredData(Flag.EXISTS))
    } catch (e: RocksDBException) {
        throw IOException(e)
    }

    @Throws(IOException::class)
    override fun remove(key: ByteArray) = try {
        database.put(key, byteArrayOf().toStoredData(Flag.DELETED))
    } catch (e: RocksDBException) {
        throw IOException(e)
    }

    @Throws(IOException::class)
    fun getInternal(key: ByteArray): Data = try {
        val bytes = database.get(key)
        when {
            bytes == null || bytes.isEmpty() -> AbsentData
            bytes[0] == Flag.DELETED.byteValue -> DeletedData(bytes.time)
            else -> ExistsData(bytes.time, bytes.value)
        }
    } catch (e: RocksDBException) {
        throw IOException(e)
    }

    fun upsertInternal(key: ByteArray, value: ByteArray): Boolean = try {
        upsert(key, value)
        true
    } catch (e: IOException) {
        false
    }

    fun removeInternal(key: ByteArray): Boolean = try {
        remove(key)
        true
    } catch (e: IOException) {
        false
    }

    override fun close() {
        database.close()
    }

    @Throws(NoSuchElementException::class)
    private fun ByteArray?.ifExists() = when {
        this == null || this.isEmpty() || this[0] == Flag.DELETED.byteValue -> throw NoSuchElementException()
        else -> this
    }

    private fun ByteArray.toStoredData(flag: Flag) =
        ByteBuffer.allocate(FLAG_LENGTH + TIME_LENGTH + this.size).apply {
            put(flag.byteValue)
            putLong(System.currentTimeMillis())
            put(this@toStoredData)
        }.array()

    private val ByteArray.time: Long
        get() = ByteBuffer.allocate(TIME_LENGTH).apply {
            put(this@time)
            flip()
        }.long

    private val ByteArray.value: ByteArray
        get() = Arrays.copyOfRange(this, FLAG_LENGTH + TIME_LENGTH, this.size)


    companion object {
        init {
            RocksDB.loadLibrary()
        }

        private const val FLAG_LENGTH = 1
        private const val TIME_LENGTH = java.lang.Long.BYTES
    }

    private enum class Flag(val byteValue: Byte) {
        EXISTS(1), DELETED(0);
    }
}


