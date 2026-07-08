package com.bradesco.saldo.batch.writer;

import com.bradesco.saldo.batch.model.AccountRecord;
import org.springframework.batch.infrastructure.item.Chunk;
import org.springframework.batch.infrastructure.item.ItemWriter;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Component;

@Component
public class KafkaLineWriter implements ItemWriter<AccountRecord> {

    private final KafkaTemplate<String, String> kafkaTemplate;
    private final String topic;

    public KafkaLineWriter(KafkaTemplate<String, String> kafkaTemplate,
                           @Value("${app.kafka-topic}") String topic) {
        this.kafkaTemplate = kafkaTemplate;
        this.topic = topic;
    }

    @Override
    public void write(Chunk<? extends AccountRecord> chunk) {
        for (AccountRecord record : chunk) {
            String payload = toJson(System.currentTimeMillis(), record.text());
            kafkaTemplate.send(topic, record.key(), payload);
        }
        kafkaTemplate.flush();
    }

    private String toJson(long timestamp, String text) {
        return "{\"timestamp\":" + timestamp + ",\"text\":\"" + escape(text) + "\"}";
    }

    private String escape(String s) {
        return s.replace("\\", "\\\\").replace("\"", "\\\"");
    }
}
