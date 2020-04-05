package com.app.aforo255.withdrawal.producer;
import com.app.aforo255.withdrawal.domain.Transaction;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;

import java.util.List;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.header.Header;
import org.apache.kafka.common.header.internals.RecordHeader;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.support.SendResult;
import org.springframework.stereotype.Component;
import org.springframework.util.concurrent.ListenableFuture;
import org.springframework.util.concurrent.ListenableFutureCallback;

@Component
public class WithdrawalEventProducer {

	String topic ="transaction-events";
	private Logger log = LoggerFactory.getLogger(WithdrawalEventProducer.class);
	
	@Autowired
	KafkaTemplate <Integer,String > KafkaTemplate ;
	@Autowired
	ObjectMapper objectMapper ;
	
	
	public void sendWithdrawalEvent(Transaction withdrawalEvent) throws JsonProcessingException {

		Integer key = withdrawalEvent.getId();
		String value = objectMapper.writeValueAsString(withdrawalEvent);

		ListenableFuture<SendResult<Integer, String>> listenableFuture = KafkaTemplate.sendDefault(key, value);
		listenableFuture.addCallback(new ListenableFutureCallback<SendResult<Integer, String>>() {
			@Override
			public void onFailure(Throwable ex) {
				handleFailure(key, value, ex);
			}

			@Override
			public void onSuccess(SendResult<Integer, String> result) {
				handleSuccess(key, value, result);
			}
		});

	}
	
	
	
	public SendResult<Integer, String> sendWithdrawalEventSynchronous(Transaction withdrawalEvent)
			throws JsonProcessingException, ExecutionException, InterruptedException, TimeoutException {

		Integer key = withdrawalEvent.getId();
		String value = objectMapper.writeValueAsString(withdrawalEvent);
		SendResult<Integer, String> sendResult = null;
		try {
			sendResult = KafkaTemplate.sendDefault(key, value).get(1, TimeUnit.SECONDS);
		} catch (ExecutionException | InterruptedException e) {
			log.error("ExecutionException/InterruptedException Sending the Message and the exception is {}",
					e.getMessage());
			throw e;
		} catch (Exception e) {
			log.error("Exception Sending the Message and the exception is {}", e.getMessage());
			throw e;
		}

		return sendResult;

	}
	
	
	public void sendWithdrawalEvent_Approach2(Transaction withdrawalEvent) throws JsonProcessingException {

		Integer key = withdrawalEvent.getId();
		String value = objectMapper.writeValueAsString(withdrawalEvent);

		ProducerRecord<Integer, String> producerRecord = buildProducerRecord(key, value, topic);

		ListenableFuture<SendResult<Integer, String>> listenableFuture = KafkaTemplate.send(producerRecord);

		listenableFuture.addCallback(new ListenableFutureCallback<SendResult<Integer, String>>() {
			@Override
			public void onFailure(Throwable ex) {
				handleFailure(key, value, ex);
			}

			@Override
			public void onSuccess(SendResult<Integer, String> result) {
				handleSuccess(key, value, result);
			}
		});
	}
	
	public ListenableFuture<SendResult<Integer,String>> sendWithdrawalEvent_Approach3(Transaction withdrawalEvent) throws JsonProcessingException {

		Integer key = withdrawalEvent.getId();
        String value = objectMapper.writeValueAsString(withdrawalEvent);

        ProducerRecord<Integer,String> producerRecord = buildProducerRecord(key, value, topic);

        ListenableFuture<SendResult<Integer,String>> listenableFuture =  KafkaTemplate.send(producerRecord);

        listenableFuture.addCallback(new ListenableFutureCallback<SendResult<Integer, String>>() {
            @Override
            public void onFailure(Throwable ex) {
                handleFailure(key, value, ex);
            }

            @Override
            public void onSuccess(SendResult<Integer, String> result) {
                handleSuccess(key, value, result);
            }
        });

        return listenableFuture;
    }
	
	
	private ProducerRecord<Integer, String> buildProducerRecord(Integer key, String value, String topic) {

		// agregando header
		List<Header> recordHeaders = List.of(new RecordHeader("withdrawal-event-source", "scanner".getBytes()));

		// return new ProducerRecord<>(topic, null, key, value, null);
		return new ProducerRecord<>(topic, null, key, value, recordHeaders);
	}
	
	
	
	private void handleFailure(Integer key, String value, Throwable ex) {
		log.error("Error Sending the Message and the exception is {}", ex.getMessage());
		try {
			throw ex;
		} catch (Throwable throwable) {
			log.error("Error in OnFailure: {}", throwable.getMessage());
		}

	}

	private void handleSuccess(Integer key, String value, SendResult<Integer, String> result) {
		log.info("Message Sent SuccessFully for the key : {} and the value is {} , partition is {}", key, value,
				result.getRecordMetadata().partition());
	}	
	
}
