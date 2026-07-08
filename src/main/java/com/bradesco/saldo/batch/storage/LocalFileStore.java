package com.bradesco.saldo.batch.storage;

import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import java.util.stream.Stream;

public class LocalFileStore implements InputStore {

    private final Path baseDir;

    public LocalFileStore(String baseDir) {
        this.baseDir = Path.of(baseDir);
    }

    @Override
    public List<StoredFile> listDataFiles() throws IOException {
        if (!Files.isDirectory(baseDir)) {
            return List.of();
        }
        try (Stream<Path> stream = Files.list(baseDir)) {
            return stream
                    .filter(p -> p.getFileName().toString().endsWith(".dat"))
                    .map(p -> {
                        try {
                            return new StoredFile(p.getFileName().toString(), Files.size(p));
                        } catch (IOException e) {
                            throw new java.io.UncheckedIOException(e);
                        }
                    })
                    .toList();
        }
    }

    @Override
    public InputStream openAt(String name, long offset) throws IOException {
        FileInputStream fis = new FileInputStream(baseDir.resolve(name).toFile());
        fis.getChannel().position(offset);
        return fis;
    }

    @Override
    public OutputStream create(String name) throws IOException {
        Files.createDirectories(baseDir);
        return Files.newOutputStream(baseDir.resolve(name));
    }
}
