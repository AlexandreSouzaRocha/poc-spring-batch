package com.bradesco.saldo.batch;

import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import com.bradesco.saldo.batch.partition.InputFilesRangePartitioner;
import com.bradesco.saldo.batch.reader.ByteRangeLineReader;
import com.bradesco.saldo.batch.storage.LocalFileStore;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import org.springframework.batch.infrastructure.item.ExecutionContext;

import static org.junit.jupiter.api.Assertions.assertEquals;

class ByteRangeLineReaderTest {

    @Test
    void partitionsCoverEveryLineExactlyOnce(@TempDir Path dir) throws Exception {
        int totalLines = 1000;
        List<String> expected = new ArrayList<>();
        StringBuilder content = new StringBuilder();
        for (int i = 0; i < totalLines; i++) {
            String line = String.format("BISD2026-07-07T23:59:59.99999900000042%07d", i);
            expected.add(line);
            content.append(line).append('\n');
        }
        Files.writeString(dir.resolve("part_0.dat"), content.toString(), StandardCharsets.UTF_8);

        LocalFileStore store = new LocalFileStore(dir.toString());
        int partitionsPerFile = 8;
        Map<String, ExecutionContext> partitions =
                new InputFilesRangePartitioner(store, partitionsPerFile).partition(partitionsPerFile);

        List<String> actual = new ArrayList<>();
        for (ExecutionContext ctx : partitions.values()) {
            actual.addAll(readPartition(store, ctx));
        }

        expected.sort(String::compareTo);
        actual.sort(String::compareTo);
        assertEquals(totalLines, actual.size(), "número de linhas lidas difere do total");
        assertEquals(expected, actual, "conteúdo lido difere do arquivo original");
    }

    private List<String> readPartition(LocalFileStore store, ExecutionContext ctx) throws Exception {
        ByteRangeLineReader reader = new ByteRangeLineReader(
                store, ctx.getString("fileName"), ctx.getLong("startByte"), ctx.getLong("endByte"));
        List<String> lines = new ArrayList<>();
        reader.open(new ExecutionContext());
        try {
            String line;
            while ((line = reader.read()) != null) {
                lines.add(line);
            }
        } finally {
            reader.close();
        }
        return lines;
    }

    @Test
    void restartResumesFromSavedPosition(@TempDir Path dir) throws Exception {
        StringBuilder content = new StringBuilder();
        for (int i = 0; i < 100; i++) {
            content.append(String.format("LINE-%03d", i)).append('\n');
        }
        Path file = dir.resolve("part_0.dat");
        Files.writeString(file, content.toString(), StandardCharsets.UTF_8);

        LocalFileStore store = new LocalFileStore(dir.toString());
        long end = Files.size(file);
        ByteRangeLineReader reader = new ByteRangeLineReader(store, "part_0.dat", 0, end);
        ExecutionContext ec = new ExecutionContext();
        reader.open(ec);
        for (int i = 0; i < 10; i++) {
            reader.read();
        }
        reader.update(ec);
        reader.close();

        ByteRangeLineReader resumed = new ByteRangeLineReader(store, "part_0.dat", 0, end);
        resumed.open(ec);
        try {
            assertEquals("LINE-010", resumed.read(), "restart deve retomar da posição salva");
        } finally {
            resumed.close();
        }
    }
}
