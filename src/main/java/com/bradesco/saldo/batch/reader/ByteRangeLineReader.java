package com.bradesco.saldo.batch.reader;

import java.io.IOException;
import java.io.RandomAccessFile;

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

    private final String filePath;
    private final long startByte;
    private final long endByte;

    private RandomAccessFile raf;

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
            this.raf = new RandomAccessFile(filePath, "r");
            if (executionContext.containsKey(POSITION_KEY)) {
                raf.seek(executionContext.getLong(POSITION_KEY));
            } else if (startByte > 0) {
                raf.seek(startByte);
                raf.readLine();
            }
        } catch (IOException e) {
            throw new ItemStreamException("Falha ao abrir partição do arquivo " + filePath, e);
        }
    }

    @Override
    public String read() throws Exception {
        if (raf.getFilePointer() > endByte) {
            return null;
        }
        return raf.readLine();
    }

    @Override
    public void update(ExecutionContext executionContext) throws ItemStreamException {
        if (raf != null) {
            try {
                executionContext.putLong(POSITION_KEY, raf.getFilePointer());
            } catch (IOException e) {
                throw new ItemStreamException("Falha ao salvar posição do reader", e);
            }
        }
    }

    @Override
    public void close() throws ItemStreamException {
        if (raf != null) {
            try {
                raf.close();
            } catch (IOException e) {
                throw new ItemStreamException("Falha ao fechar arquivo " + filePath, e);
            } finally {
                raf = null;
            }
        }
    }
}
