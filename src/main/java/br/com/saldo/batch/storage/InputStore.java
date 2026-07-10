package br.com.saldo.batch.storage;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.List;

public interface InputStore {

    record StoredFile(String name, long length) {
    }

    List<StoredFile> listDataFiles() throws IOException;

    InputStream openAt(String name, long offset) throws IOException;

    OutputStream create(String name) throws IOException;

    void moveToErrorFolder(String name) throws IOException;
}
