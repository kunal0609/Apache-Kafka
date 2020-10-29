package com.demo.Kafka;

import java.util.Date;
import java.util.Properties;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.ByteArraySerializer;
import org.apache.kafka.common.serialization.StringSerializer;

public class MessageSender {

	Properties prop = new Properties();

	private void init() throws InterruptedException{
		prop.setProperty("bootstrap.servers", "localhost:9092");
		prop.setProperty("kafka.topic.name", "Whatsapp");
		KafkaProducer<String, byte[]> producer = new KafkaProducer<String, byte[]>(this.prop, new StringSerializer(),new ByteArraySerializer());
		
		for(int i=0; i<1000; i++) {
			byte[] payload = (i+ "Message from java code"+new Date()).getBytes();
			System.out.println(i+ "Message from java code"+new Date());
			ProducerRecord<String, byte[]> record = new ProducerRecord<String, byte[]>(prop.getProperty("kafka.topic.name"), payload);
			producer.send(record);
			Thread.sleep(1000);
		}
		producer.close();
	}
	
	public static void main(String[] args) throws InterruptedException {
		MessageSender sender = new MessageSender();
		sender.init();
	}
	
}
