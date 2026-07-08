package com.bradesco.saldo.batch.generator;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.util.concurrent.ThreadLocalRandom;

import com.bradesco.saldo.batch.model.RecordLayout;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;

@Service
public class AccountFileGenerator {

    private static final Logger log = LoggerFactory.getLogger(AccountFileGenerator.class);
    private static final int DIGITS = 10;

    public record GenerationResult(int files, long totalLines, int recordLength,
                                   String directory, long elapsedMs) {
    }

    public GenerationResult generate(long linesPerDigit, String directory, String date, int recordLength)
            throws IOException {
        int length = Math.max(RecordLayout.MEANINGFUL_LENGTH, recordLength);
        File dir = new File(directory);
        if (!dir.exists() && !dir.mkdirs()) {
            throw new IOException("Não foi possível criar o diretório " + dir.getAbsolutePath());
        }

        long start = System.currentTimeMillis();
        long total = 0;
        for (int digit = 0; digit < DIGITS; digit++) {
            File file = new File(dir, "part_" + digit + ".dat");
            try (BufferedWriter writer = new BufferedWriter(new FileWriter(file), 1 << 20)) {
                ThreadLocalRandom rnd = ThreadLocalRandom.current();
                StringBuilder sb = new StringBuilder(length + 1);
                for (long i = 0; i < linesPerDigit; i++) {
                    buildLine(sb, rnd, date, digit, length);
                    writer.write(sb.toString());
                }
            }
            total += linesPerDigit;
        }
        long elapsed = System.currentTimeMillis() - start;
        log.info("Gerados {} arquivos, {} linhas ({} bytes/linha) em {} ({} ms)",
                DIGITS, total, length, dir.getAbsolutePath(), elapsed);
        return new GenerationResult(DIGITS, total, length, dir.getAbsolutePath(), elapsed);
    }

    private void buildLine(StringBuilder sb, ThreadLocalRandom rnd, String date, int digit, int length) {
        sb.setLength(0);
        sb.append(RecordLayout.PREFIX).append(date).append(RecordLayout.TIMESTAMP_LITERAL);
        appendPadded(sb, rnd.nextInt(10_000), RecordLayout.AGENCY_LENGTH);
        appendPadded(sb, rnd.nextInt(1_000_000), RecordLayout.ACCOUNT_LENGTH - 1);
        sb.append((char) ('0' + digit));
        while (sb.length() < length) {
            sb.append((char) ('0' + rnd.nextInt(10)));
        }
        sb.append('\n');
    }

    private void appendPadded(StringBuilder sb, int value, int width) {
        String s = Integer.toString(value);
        for (int i = s.length(); i < width; i++) {
            sb.append('0');
        }
        sb.append(s);
    }
}
