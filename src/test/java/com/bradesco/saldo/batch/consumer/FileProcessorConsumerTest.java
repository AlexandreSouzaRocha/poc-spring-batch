package com.bradesco.saldo.batch.consumer;

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

class FileProcessorConsumerTest {

    @Test
    void sendsToDlqOnlyAfterMaxAttemptsReached() {
        assertFalse(FileProcessorConsumer.shouldSendToDlq(0, 3));
        assertFalse(FileProcessorConsumer.shouldSendToDlq(2, 3));
        assertTrue(FileProcessorConsumer.shouldSendToDlq(3, 3));
        assertTrue(FileProcessorConsumer.shouldSendToDlq(5, 3));
    }
}
