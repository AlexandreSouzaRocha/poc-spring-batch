package br.com.saldo.batch.writer;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;

import br.com.saldo.batch.model.AccountRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.batch.infrastructure.item.Chunk;
import org.springframework.batch.infrastructure.item.ItemWriter;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.support.SendResult;
import org.springframework.stereotype.Component;

@Component
public class KafkaLineWriter implements ItemWriter<AccountRecord> {

    private static final Logger log = LoggerFactory.getLogger(KafkaLineWriter.class);

    private final KafkaTemplate<String, String> kafkaTemplate;
    private final String topic;

    public KafkaLineWriter(KafkaTemplate<String, String> kafkaTemplate,
                           @Value("${app.kafka-topic}") String topic) {
        this.kafkaTemplate = kafkaTemplate;
        this.topic = topic;
    }

    @Override
    public void write(Chunk<? extends AccountRecord> chunk) {
        int size = chunk.size();
        String firstKey = size > 0 ? chunk.getItems().get(0).key() : null;
        String lastKey = size > 0 ? chunk.getItems().get(size - 1).key() : null;
        log.debug("Enviando chunk de {} registros para o tópico {} (chaves {}..{})", size, topic, firstKey, lastKey);

        List<CompletableFuture<SendResult<String, String>>> futures = new ArrayList<>(size);
        for (AccountRecord record : chunk) {
            String payload = toJson(System.currentTimeMillis(), record.text());
            futures.add(kafkaTemplate.send(topic, record.key(), payload));
        }
        kafkaTemplate.flush();

        try {
            CompletableFuture.allOf(futures.toArray(new CompletableFuture[0])).join();
        } catch (CompletionException e) {
            if (e.getCause() instanceof RuntimeException re) {
                throw re;
            }
            throw e;
        }

        log.info("Chunk de {} registros publicado com sucesso no tópico {} (chaves {}..{})", size, topic, firstKey, lastKey);
    }

    private String toJson(long timestamp, String text) {
        return "{\"timestamp\":" + timestamp + ",\"text\":\"" + escape(text) + "\"}";
    }

    private String escape(String s) {
        return s.replace("\\", "\\\\").replace("\"", "\\\"");
    }
}
