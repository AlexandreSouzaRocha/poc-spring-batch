package com.bradesco.saldo.batch.partition;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import com.bradesco.saldo.batch.storage.InputStore;
import com.bradesco.saldo.batch.storage.InputStore.StoredFile;

import org.springframework.batch.core.partition.Partitioner;
import org.springframework.batch.infrastructure.item.ExecutionContext;

public class InputFilesRangePartitioner implements Partitioner {

    private static final Pattern DIGIT_FILE = Pattern.compile("part_(\\d)\\.dat");

    private final InputStore store;
    private final int partitionsPerFile;
    private final int digitFrom;
    private final int digitTo;

    public InputFilesRangePartitioner(InputStore store, int partitionsPerFile) {
        this(store, partitionsPerFile, 0, 9);
    }

    public InputFilesRangePartitioner(InputStore store, int partitionsPerFile, int digitFrom, int digitTo) {
        this.store = store;
        this.partitionsPerFile = Math.max(1, partitionsPerFile);
        this.digitFrom = digitFrom;
        this.digitTo = digitTo;
    }

    @Override
    public Map<String, ExecutionContext> partition(int gridSize) {
        List<StoredFile> files;
        try {
            files = store.listDataFiles().stream()
                    .filter(f -> matchesShard(f.name()))
                    .sorted(Comparator.comparing(StoredFile::name))
                    .toList();
        } catch (IOException e) {
            throw new UncheckedIOException("Falha ao listar arquivos de entrada", e);
        }
        if (files.isEmpty()) {
            throw new IllegalStateException("Nenhum arquivo *.dat encontrado no storage"
                    + " para o intervalo de dígitos [" + digitFrom + "-" + digitTo + "]");
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

    private boolean matchesShard(String fileName) {
        Matcher matcher = DIGIT_FILE.matcher(fileName);
        if (!matcher.matches()) {
            return false;
        }
        int digit = Integer.parseInt(matcher.group(1));
        return digit >= digitFrom && digit <= digitTo;
    }
}
