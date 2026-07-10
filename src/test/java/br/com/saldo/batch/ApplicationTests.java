package br.com.saldo.batch;

import br.com.saldo.batch.model.AccountRecord;
import br.com.saldo.batch.model.RecordLayout;
import br.com.saldo.batch.processor.LineProcessor;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;

class ApplicationTests {

    private final LineProcessor processor = new LineProcessor();

    @Test
    void extractsAgencyAndAccountFromFixed260ByteRecord() {
        String meaningful = "BISD2026-07-07T23:59:59.999999000000420012345";
        StringBuilder sb = new StringBuilder(meaningful);
        while (sb.length() < RecordLayout.RECORD_LENGTH) {
            sb.append(' ');
        }
        String line = sb.toString();
        assertEquals(260, line.length());

        AccountRecord record = processor.process(line);

        assertEquals("0042-0012345", record.key());
        assertEquals(line, record.text(), "o text publicado deve ser a linha completa (260 bytes)");
        assertEquals(260, record.text().length());
    }

    @Test
    void ignoresShortOrNullLines() {
        assertNull(processor.process(null));
        assertNull(processor.process("curta"));
    }
}
