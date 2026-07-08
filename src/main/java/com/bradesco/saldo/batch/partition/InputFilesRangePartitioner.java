package com.bradesco.saldo.batch.partition;

import java.io.File;
import java.util.Arrays;
import java.util.Comparator;
import java.util.HashMap;
import java.util.Map;

import org.springframework.batch.core.partition.Partitioner;
import org.springframework.batch.infrastructure.item.ExecutionContext;

public class InputFilesRangePartitioner implements Partitioner {

    private final File inputDir;
    private final int partitionsPerFile;

    public InputFilesRangePartitioner(String inputDir, int partitionsPerFile) {
        this.inputDir = new File(inputDir);
        this.partitionsPerFile = Math.max(1, partitionsPerFile);
    }

    @Override
    public Map<String, ExecutionContext> partition(int gridSize) {
        File[] files = inputDir.listFiles((dir, name) -> name.endsWith(".dat"));
        if (files == null || files.length == 0) {
            throw new IllegalStateException("Nenhum arquivo *.dat encontrado em " + inputDir.getAbsolutePath());
        }
        Arrays.sort(files, Comparator.comparing(File::getName));

        Map<String, ExecutionContext> partitions = new HashMap<>();
        for (File file : files) {
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
                context.putString("fileName", file.getAbsolutePath());
                context.putLong("startByte", start);
                context.putLong("endByte", end);
                partitions.put(file.getName() + "#" + i, context);
            }
        }
        return partitions;
    }
}
