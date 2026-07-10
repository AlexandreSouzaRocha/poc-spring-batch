package br.com.saldo.batch;

import br.com.saldo.batch.partition.FileNaming;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;

class FileNamingTest {

    @Test
    void buildsFileNameForDigit() {
        assertEquals("1720471234567_part_7.dat", FileNaming.fileNameForDigit("1720471234567", 7));
    }
}
