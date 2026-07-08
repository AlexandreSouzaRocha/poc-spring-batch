package com.bradesco.saldo.batch.partition;

import java.io.File;
import java.util.Arrays;
import java.util.Comparator;
import java.util.HashMap;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.springframework.batch.core.partition.Partitioner;
import org.springframework.batch.infrastructure.item.ExecutionContext;

public class InputFilesRangePartitioner implements Partitioner {

    private static final Pattern DIGIT_FILE = Pattern.compile("part_(\\d)\\.dat");

    private final File inputDir;
    private final int partitionsPerFile;
    private final int digitFrom;
    private final int digitTo;

    public InputFilesRangePartitioner(String inputDir, int partitionsPerFile) {
        this(inputDir, partitionsPerFile, 0, 9);
    }

    public InputFilesRangePartitioner(String inputDir, int partitionsPerFile, int digitFrom, int digitTo) {
        this.inputDir = new File(inputDir);
        this.partitionsPerFile = Math.max(1, partitionsPerFile);
        this.digitFrom = digitFrom;
        this.digitTo = digitTo;
    }

    @Override
    public Map<String, ExecutionContext> partition(int gridSize) {
        File[] files = inputDir.listFiles((dir, name) -> matchesShard(name));
        if (files == null || files.length == 0) {
            throw new IllegalStateException("Nenhum arquivo *.dat encontrado em " + inputDir.getAbsolutePath()
                    + " para o intervalo de dígitos [" + digitFrom + "-" + digitTo + "]");
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

    private boolean matchesShard(String fileName) {
        Matcher matcher = DIGIT_FILE.matcher(fileName);
        if (!matcher.matches()) {
            return false;
        }
        int digit = Integer.parseInt(matcher.group(1));
        return digit >= digitFrom && digit <= digitTo;
    }
}
