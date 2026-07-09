package com.bradesco.saldo.batch;

import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Map;

import com.bradesco.saldo.batch.partition.InputFilesRangePartitioner;
import com.bradesco.saldo.batch.storage.LocalFileStore;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import org.springframework.batch.infrastructure.item.ExecutionContext;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

class InputFilesRangePartitionerTest {

    @Test
    void singleFileModeOnlyPartitionsTheNamedFile(@TempDir Path dir) throws Exception {
        Files.writeString(dir.resolve("part_3.dat"), "a".repeat(1000), StandardCharsets.US_ASCII);
        Files.writeString(dir.resolve("part_7.dat"), "b".repeat(1000), StandardCharsets.US_ASCII);
        LocalFileStore store = new LocalFileStore(dir.toString());

        Map<String, ExecutionContext> partitions =
                InputFilesRangePartitioner.forSingleFile(store, 4, "part_3.dat").partition(4);

        assertTrue(partitions.keySet().stream().allMatch(k -> k.startsWith("part_3.dat#")),
                "só deve gerar partições do arquivo pedido");
        assertEquals(4, partitions.size());
    }

    @Test
    void singleFileModeFailsWhenFileDoesNotExist(@TempDir Path dir) throws Exception {
        Files.writeString(dir.resolve("part_3.dat"), "a".repeat(1000), StandardCharsets.US_ASCII);
        LocalFileStore store = new LocalFileStore(dir.toString());

        InputFilesRangePartitioner partitioner = InputFilesRangePartitioner.forSingleFile(store, 4, "part_9.dat");
        assertThrows(IllegalStateException.class, () -> partitioner.partition(4));
    }

    @Test
    void digitRangeModeStillWorks(@TempDir Path dir) throws Exception {
        Files.writeString(dir.resolve("1720471234567_part_0.dat"), "a".repeat(500), StandardCharsets.US_ASCII);
        Files.writeString(dir.resolve("1720471234567_part_5.dat"), "b".repeat(500), StandardCharsets.US_ASCII);
        LocalFileStore store = new LocalFileStore(dir.toString());

        Map<String, ExecutionContext> partitions =
                new InputFilesRangePartitioner(store, 2, 0, 4).partition(2);

        assertTrue(partitions.keySet().stream().allMatch(k -> k.startsWith("1720471234567_part_0.dat#")),
                "range 0-4 não deve incluir part_5.dat");
    }
}
