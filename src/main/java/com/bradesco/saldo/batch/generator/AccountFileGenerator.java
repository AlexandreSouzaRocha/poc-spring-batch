package com.bradesco.saldo.batch.generator;

import java.io.BufferedOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.io.UncheckedIOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.ThreadLocalRandom;
import java.util.stream.IntStream;

import com.bradesco.saldo.batch.model.RecordLayout;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;

@Service
public class AccountFileGenerator {

    private static final Logger log = LoggerFactory.getLogger(AccountFileGenerator.class);
    private static final int DIGITS = 10;
    private static final int BUFFER_SIZE = 1 << 20;

    public record GenerationResult(int files, long totalLines, int recordLength,
                                   String directory, long elapsedMs) {
    }

    public GenerationResult generate(long linesPerDigit, String directory, String date, int recordLength)
            throws IOException {
        int length = Math.max(RecordLayout.MEANINGFUL_LENGTH, recordLength);
        Path dir = Path.of(directory);
        Files.createDirectories(dir);

        byte[] header = (RecordLayout.PREFIX + date + RecordLayout.TIMESTAMP_LITERAL)
                .getBytes(StandardCharsets.US_ASCII);

        long start = System.currentTimeMillis();
        int threads = Math.min(DIGITS, Math.max(1, Runtime.getRuntime().availableProcessors()));
        ExecutorService executor = Executors.newFixedThreadPool(threads);
        try {
            var futures = IntStream.range(0, DIGITS)
                    .<Future<?>>mapToObj(digit -> executor.submit(
                            () -> writeDigitFile(dir, digit, linesPerDigit, header, length)))
                    .toList();
            for (Future<?> future : futures) {
                future.get();
            }
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new IOException("Geração interrompida", e);
        } catch (ExecutionException e) {
            throw new IOException("Falha ao gerar arquivos de teste", e.getCause());
        } finally {
            executor.shutdown();
        }

        long total = linesPerDigit * DIGITS;
        long elapsed = System.currentTimeMillis() - start;
        log.info("Gerados {} arquivos, {} linhas ({} bytes/linha) em {} ({} ms)",
                DIGITS, total, length, dir.toAbsolutePath(), elapsed);
        return new GenerationResult(DIGITS, total, length, dir.toAbsolutePath().toString(), elapsed);
    }

    private void writeDigitFile(Path dir, int digit, long linesPerDigit, byte[] header, int length) {
        Path file = dir.resolve("part_" + digit + ".dat");
        int fillerLength = length - RecordLayout.MEANINGFUL_LENGTH;
        ThreadLocalRandom rnd = ThreadLocalRandom.current();
        byte[] line = new byte[length + 1];
        byte[] filler = new byte[Math.max(0, fillerLength)];
        line[length] = '\n';

        try (OutputStream out = new BufferedOutputStream(Files.newOutputStream(file), BUFFER_SIZE)) {
            for (long i = 0; i < linesPerDigit; i++) {
                buildLine(line, header, filler, rnd, digit, fillerLength);
                out.write(line);
            }
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    private void buildLine(byte[] line, byte[] header, byte[] filler, ThreadLocalRandom rnd,
                           int digit, int fillerLength) {
        System.arraycopy(header, 0, line, 0, header.length);
        int pos = header.length;
        pos = writeDigits(line, pos, rnd.nextInt(10_000), RecordLayout.AGENCY_LENGTH);
        pos = writeDigits(line, pos, rnd.nextInt(1_000_000), RecordLayout.ACCOUNT_LENGTH - 1);
        line[pos++] = (byte) ('0' + digit);

        if (fillerLength > 0) {
            rnd.nextBytes(filler);
            for (int i = 0; i < fillerLength; i++) {
                line[pos + i] = (byte) ('0' + (filler[i] & 0xFF) % 10);
            }
        }
    }

    private int writeDigits(byte[] line, int pos, int value, int width) {
        for (int i = width - 1; i >= 0; i--) {
            line[pos + i] = (byte) ('0' + (value % 10));
            value /= 10;
        }
        return pos + width;
    }
}
