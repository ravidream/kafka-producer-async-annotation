package com.example.producer;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.kafka.core.KafkaProducerException;
import org.springframework.kafka.core.KafkaSendCallback;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.support.SendResult;
import org.springframework.scheduling.annotation.Async;
import org.springframework.stereotype.Component;
import org.springframework.util.concurrent.ListenableFuture;

@Component
public class KafkaMessageProducer {
	
	private static Logger LOGGER = LoggerFactory.getLogger(KafkaMessageProducer.class);
	
	private static final String TOPIC = "test";
	
	@Autowired
	@Qualifier("producer")
	private KafkaTemplate<String, String> kafkaTemplate;

	@Async("asyncExecutor")
	public void sendMessage(String message) {
		ListenableFuture<SendResult<String,String>> future = kafkaTemplate.send(TOPIC, message);

        future.addCallback(new KafkaSendCallback<String, String>() {

            @Override
            public void onFailure(KafkaProducerException ex) {
            	LOGGER.warn("Message could not be delivered. " + ex.getMessage());
            }

            @Override
            public void onSuccess(SendResult<String, String> result) {
            	LOGGER.info("Your message was delivered with following offset: " + result.getRecordMetadata().offset());
            	LOGGER.info("Thread Name : {}", Thread.currentThread());
            }
        });
	}
}