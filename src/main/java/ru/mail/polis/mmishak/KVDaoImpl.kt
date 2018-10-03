package ru.mail.polis.mmishak

import org.rocksdb.Options
import org.rocksdb.RocksDB
import org.rocksdb.RocksDBException
import ru.mail.polis.KVDao
import java.io.File
import java.io.IOException

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
        database.get(key) ?: throw NoSuchElementException()
    } catch (e: RocksDBException) {
        throw IOException(e)
    }

    @Throws(IOException::class)
    override fun upsert(key: ByteArray, value: ByteArray) = try {
        database.put(key, value)
    } catch (e: RocksDBException) {
        throw IOException(e)
    }

    @Throws(IOException::class)
    override fun remove(key: ByteArray) = try {
        database.delete(key)
    } catch (e: RocksDBException) {
        throw IOException(e)
    }

    override fun close() {
        database.close()
    }

    companion object {
        init {
            RocksDB.loadLibrary()
        }
    }
}
