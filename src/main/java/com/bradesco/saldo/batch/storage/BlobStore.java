package com.bradesco.saldo.batch.storage;

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

    private final BlobContainerClient container;

    public BlobStore(BlobContainerClient container) {
        this.container = container;
    }

    @Override
    public List<StoredFile> listDataFiles() {
        return container.listBlobs().stream()
                .filter(b -> b.getName().endsWith(".dat"))
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

    /**
     * Grava em um arquivo temporário local e envia via {@code uploadFromFile} no
     * {@code close()}: o SDK faz upload em blocos direto do disco (streaming real,
     * sem acumular o arquivo inteiro em heap) — mais robusto que escrever direto num
     * {@code BlobOutputStream} para arquivos grandes.
     */
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
}
