package com.bradesco.saldo.batch.reader;

import java.io.BufferedReader;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.nio.charset.StandardCharsets;

import org.springframework.batch.core.configuration.annotation.StepScope;
import org.springframework.batch.infrastructure.item.ExecutionContext;
import org.springframework.batch.infrastructure.item.ItemStreamException;
import org.springframework.batch.infrastructure.item.ItemStreamReader;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

@Component
@StepScope
public class ByteRangeLineReader implements ItemStreamReader<String> {

    private static final String POSITION_KEY = "byteRangeReader.position";
    private static final int BUFFER_SIZE = 1 << 16;

    private final String filePath;
    private final long startByte;
    private final long endByte;

    private BufferedReader reader;
    private long position;

    public ByteRangeLineReader(
            @Value("#{stepExecutionContext['fileName']}") String filePath,
            @Value("#{stepExecutionContext['startByte']}") long startByte,
            @Value("#{stepExecutionContext['endByte']}") long endByte) {
        this.filePath = filePath;
        this.startByte = startByte;
        this.endByte = endByte;
    }

    @Override
    public void open(ExecutionContext executionContext) throws ItemStreamException {
        try {
            long from = executionContext.containsKey(POSITION_KEY)
                    ? executionContext.getLong(POSITION_KEY)
                    : startByte;

            FileInputStream fis = new FileInputStream(filePath);
            fis.getChannel().position(from);
            reader = new BufferedReader(
                    new InputStreamReader(fis, StandardCharsets.ISO_8859_1), BUFFER_SIZE);
            position = from;

            if (!executionContext.containsKey(POSITION_KEY) && startByte > 0) {
                String partial = reader.readLine();
                if (partial != null) {
                    position += partial.length() + 1;
                }
            }
        } catch (IOException e) {
            throw new ItemStreamException("Falha ao abrir partição do arquivo " + filePath, e);
        }
    }

    @Override
    public String read() throws Exception {
        if (position > endByte) {
            return null;
        }
        String line = reader.readLine();
        if (line != null) {
            position += line.length() + 1;
        }
        return line;
    }

    @Override
    public void update(ExecutionContext executionContext) throws ItemStreamException {
        executionContext.putLong(POSITION_KEY, position);
    }

    @Override
    public void close() throws ItemStreamException {
        if (reader != null) {
            try {
                reader.close();
            } catch (IOException e) {
                throw new ItemStreamException("Falha ao fechar arquivo " + filePath, e);
            } finally {
                reader = null;
            }
        }
    }
}
