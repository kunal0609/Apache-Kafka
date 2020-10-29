package com.demo.Kafka;

import java.util.Arrays;
import java.util.Properties;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.StringDeserializer;

public class MessageReceiver {

	static final String TOPIC = "Whatsapp";
	static final String GROUP = "Whatsapp_group";
	
	public static void main(String[] args) {
		
		Properties props = new Properties();
		props.put("bootstrap.servers", "localhost:9092");
		props.put("group.id", GROUP);
		props.put("auto.commit.interval.ms", "1000");
		props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);

		try(KafkaConsumer<String, String> consumer = new KafkaConsumer<>(props);){
			consumer.subscribe(Arrays.asList(TOPIC));
			
			for(int i=0; i<1000; i++) {
				ConsumerRecords<String, String> records = consumer.poll(1000L);
				System.out.println("Size: "+records.count());
				for(ConsumerRecord<String, String> record:records) {
					System.out.println("Received a message: "+record.value());
				}
			}
		
		}
		System.out.println("End");
	}
}
