package ru.mail.polis.mmishak;

import org.jetbrains.annotations.NotNull;
import ru.mail.polis.KVDao;

import javax.xml.bind.DatatypeConverter;
import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.util.NoSuchElementException;

public class KVDaoImpl implements KVDao {

    private static final String SEPARATOR = "/";

    private File storageDirectory;

    public KVDaoImpl(File storage) {
        this.storageDirectory = storage;
    }

    @NotNull
    @Override
    public byte[] get(@NotNull byte[] key) throws NoSuchElementException, IOException {
        File file = getFile(key);
        if (!file.exists())
            throw new NoSuchElementException("File with key'" + new String(key) + "' not exits");
        return Files.readAllBytes(file.toPath());
    }

    @Override
    public void upsert(@NotNull byte[] key, @NotNull byte[] value) throws IOException {
        File file = getFile(key);
        if (!file.exists()) {
            final boolean success = file.createNewFile();
            if (!success)
                throw new IOException();
        }
        Files.write(file.toPath(), value);
    }

    @Override
    public void remove(@NotNull byte[] key) throws IOException {
        File file = getFile(key);
        final boolean success = !file.exists() || file.delete();
        if (!success)
            throw new IOException();
    }

    @Override
    public void close() {
        storageDirectory = null;
    }

    @NotNull
    private File getFile(@NotNull byte[] key) {
        return new File(storageDirectory.getAbsolutePath() + SEPARATOR + keyToString(key));
    }

    private String keyToString(byte[] key) {
        return DatatypeConverter.printHexBinary(key);
    }
}
