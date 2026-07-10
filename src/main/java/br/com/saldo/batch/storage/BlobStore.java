package br.com.saldo.batch.storage;

import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;

import com.azure.storage.blob.BlobContainerClient;
import com.azure.storage.blob.models.BlobRange;
import com.azure.storage.blob.options.BlobInputStreamOptions;

public class BlobStore implements InputStore {

    private static final int READ_BLOCK_SIZE = 1 << 22;
    private static final String ERROR_PREFIX = "errors/";

    private final BlobContainerClient container;

    public BlobStore(BlobContainerClient container) {
        this.container = container;
    }

    @Override
    public List<StoredFile> listDataFiles() {
        return container.listBlobs().stream()
                .filter(b -> b.getName().endsWith(".dat") && !b.getName().startsWith(ERROR_PREFIX))
                .map(b -> new StoredFile(b.getName(), b.getProperties().getContentLength()))
                .toList();
    }

    @Override
    public InputStream openAt(String name, long offset) {
        return container.getBlobClient(name).openInputStream(
                new BlobInputStreamOptions()
                        .setRange(new BlobRange(offset))
                        .setBlockSize(READ_BLOCK_SIZE));
    }

    @Override
    public OutputStream create(String name) throws IOException {
        Path tempFile = Files.createTempFile("blob-upload-", ".tmp");
        FileOutputStream out = new FileOutputStream(tempFile.toFile());
        return new OutputStream() {
            @Override
            public void write(int b) throws IOException {
                out.write(b);
            }

            @Override
            public void write(byte[] b, int off, int len) throws IOException {
                out.write(b, off, len);
            }

            @Override
            public void close() throws IOException {
                out.close();
                try {
                    container.getBlobClient(name).uploadFromFile(tempFile.toString(), true);
                } finally {
                    Files.deleteIfExists(tempFile);
                }
            }
        };
    }

    @Override
    public void moveToErrorFolder(String name) throws IOException {
        Path tempFile = Files.createTempFile("blob-error-move-", ".tmp");
        try {
            try (InputStream in = openAt(name, 0); OutputStream out = Files.newOutputStream(tempFile)) {
                in.transferTo(out);
            }
            container.getBlobClient(ERROR_PREFIX + name).uploadFromFile(tempFile.toString(), true);
            container.getBlobClient(name).deleteIfExists();
        } finally {
            Files.deleteIfExists(tempFile);
        }
    }
}
