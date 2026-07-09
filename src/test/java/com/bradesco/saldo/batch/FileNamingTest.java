package com.bradesco.saldo.batch;

import com.bradesco.saldo.batch.partition.FileNaming;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

class FileNamingTest {

    @Test
    void matchesValidPartFiles() {
        assertTrue(FileNaming.matches("1720471234567_part_0.dat"));
        assertTrue(FileNaming.matches("1720471234567_part_9.dat"));
        assertFalse(FileNaming.matches("1720471234567_part_10.dat"));
        assertFalse(FileNaming.matches("errors/1720471234567_part_0.dat"));
        assertFalse(FileNaming.matches("part_0.dat"));
        assertFalse(FileNaming.matches("random.txt"));
    }

    @Test
    void extractsDigitFromFileName() {
        assertEquals(3, FileNaming.extractDigit("1720471234567_part_3.dat"));
    }

    @Test
    void rejectsExtractDigitOnInvalidName() {
        assertThrows(IllegalArgumentException.class, () -> FileNaming.extractDigit("not-a-part-file.dat"));
    }

    @Test
    void buildsFileNameForDigit() {
        assertEquals("1720471234567_part_7.dat", FileNaming.fileNameForDigit("1720471234567", 7));
    }
}
