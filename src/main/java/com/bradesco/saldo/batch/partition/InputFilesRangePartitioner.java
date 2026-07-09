package com.bradesco.saldo.batch.partition;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.function.Predicate;

import com.bradesco.saldo.batch.storage.InputStore;
import com.bradesco.saldo.batch.storage.InputStore.StoredFile;

import org.springframework.batch.core.partition.Partitioner;
import org.springframework.batch.infrastructure.item.ExecutionContext;

public class InputFilesRangePartitioner implements Partitioner {

    private final InputStore store;
    private final int partitionsPerFile;
    private final Predicate<String> fileFilter;
    private final String filterDescription;

    public static InputFilesRangePartitioner forSingleFile(InputStore store, int partitionsPerFile, String fileName) {
        return new InputFilesRangePartitioner(store, partitionsPerFile,
                fileName::equals, "arquivo '" + fileName + "'");
    }

    InputFilesRangePartitioner(InputStore store, int partitionsPerFile,
                               Predicate<String> fileFilter, String filterDescription) {
        this.store = store;
        this.partitionsPerFile = Math.max(1, partitionsPerFile);
        this.fileFilter = fileFilter;
        this.filterDescription = filterDescription;
    }

    @Override
    public Map<String, ExecutionContext> partition(int gridSize) {
        List<StoredFile> files;
        try {
            files = store.listDataFiles().stream()
                    .filter(f -> fileFilter.test(f.name()))
                    .sorted(Comparator.comparing(StoredFile::name))
                    .toList();
        } catch (IOException e) {
            throw new UncheckedIOException("Falha ao listar arquivos de entrada", e);
        }
        if (files.isEmpty()) {
            throw new IllegalStateException("Nenhum arquivo *.dat encontrado no storage para " + filterDescription);
        }

        Map<String, ExecutionContext> partitions = new HashMap<>();
        for (StoredFile file : files) {
            long length = file.length();
            if (length == 0) {
                continue;
            }
            long slice = Math.max(1, length / partitionsPerFile);
            for (int i = 0; i < partitionsPerFile; i++) {
                long start = i * slice;
                if (start >= length) {
                    break;
                }
                long end = (i == partitionsPerFile - 1) ? length : Math.min(length, (i + 1) * slice);

                ExecutionContext context = new ExecutionContext();
                context.putString("fileName", file.name());
                context.putLong("startByte", start);
                context.putLong("endByte", end);
                partitions.put(file.name() + "#" + i, context);
            }
        }
        return partitions;
    }
}
