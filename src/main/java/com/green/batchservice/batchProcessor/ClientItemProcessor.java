package com.green.batchservice.batchProcessor;

import com.green.batchservice.entity.Client;
import org.springframework.batch.item.ItemProcessor;
import org.springframework.stereotype.Component;

@Component
public class ClientItemProcessor implements ItemProcessor<Client,Client> {

    @Override
    public Client process(Client client) throws Exception {
        return client;
    }
}
