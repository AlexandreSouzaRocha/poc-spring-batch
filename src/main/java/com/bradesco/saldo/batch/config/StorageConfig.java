package com.bradesco.saldo.batch.config;

import com.azure.storage.blob.BlobContainerClient;
import com.azure.storage.blob.BlobServiceClientBuilder;
import com.azure.storage.common.StorageSharedKeyCredential;
import com.bradesco.saldo.batch.storage.BlobStore;
import com.bradesco.saldo.batch.storage.InputStore;
import com.bradesco.saldo.batch.storage.LocalFileStore;

import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

@Configuration
public class StorageConfig {

    @Bean
    @ConditionalOnProperty(name = "app.storage", havingValue = "file", matchIfMissing = true)
    public InputStore localFileStore(@Value("${app.input-dir}") String inputDir) {
        return new LocalFileStore(inputDir);
    }

    @Bean
    @ConditionalOnProperty(name = "app.storage", havingValue = "blob")
    public InputStore blobStore(
            @Value("${app.blob.endpoint}") String endpoint,
            @Value("${app.blob.account-name}") String accountName,
            @Value("${app.blob.account-key}") String accountKey,
            @Value("${app.blob.container}") String containerName) {
        StorageSharedKeyCredential credential = new StorageSharedKeyCredential(accountName, accountKey);
        BlobContainerClient container = new BlobServiceClientBuilder()
                .endpoint(endpoint)
                .credential(credential)
                .buildClient()
                .getBlobContainerClient(containerName);
        container.createIfNotExists();
        return new BlobStore(container);
    }
}
