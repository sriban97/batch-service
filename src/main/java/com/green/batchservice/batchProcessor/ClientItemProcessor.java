package com.green.batchservice.batchProcessor;

import com.green.batchservice.entity.Client;
import lombok.extern.slf4j.Slf4j;
import org.springframework.batch.item.ItemProcessor;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.support.SendResult;
import org.springframework.stereotype.Component;
import org.springframework.util.ObjectUtils;

import java.util.concurrent.CompletableFuture;

@Slf4j
@Component
public class ClientItemProcessor implements ItemProcessor<Client, Client> {

    @Autowired
    private KafkaTemplate<String, Client> kafkaTemplate;

    @Override
    public Client process(Client client) {
        CompletableFuture<SendResult<String, Client>> sendResultListenableFuture = kafkaTemplate.sendDefault("client_info", client);
        sendResultListenableFuture.whenCompleteAsync((success, error) -> {
            if (!ObjectUtils.isEmpty(error)) {
                log.error("Message Sent Failed : {}", error.getMessage());
            } else {
                log.info("Message Sent Successfully : Topic: {}, Message: {}", success.getProducerRecord().topic(), success.getProducerRecord().value());
            }
        });
        return client;
    }
}
