package edu.ds2019.workLoadGenerator;
import java.util.Properties;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;

public class ForestFireProducer/* implements Runnable*/ {
	
	
	static  KafkaProducer kafkaProducer=null;
	private IOTDevice device;
	public ForestFireProducer(IOTDevice device) {
		this.device = device;
	}
	
	/*@Override
	public void run() {
		while(true) {
			ObjectMapper mapper=new ObjectMapper();
			try {
				String json = mapper.writeValueAsString(this.device);
				kafkaProducer(json);
			} catch (JsonProcessingException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
			try {
				Thread.sleep(5000);
			} catch (InterruptedException e1) {
				// TODO Auto-generated catch block
				e1.printStackTrace();
			}
		}
	}
	*/
	public static void kafkaProducer(String jsonString, Integer id) {
		if(kafkaProducer==null) {
			Properties properties = new Properties();
			properties.put("bootstrap.servers", "localhost:9092");
			properties.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
			properties.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");
			kafkaProducer = new KafkaProducer(properties);
		}
		
		try {
			//System.out.println("jsonString generate:"+jsonString);
			kafkaProducer.send(new ProducerRecord("generateLoad",/* String.valueOf(id),*/ jsonString));

		} catch (Exception e) {
			e.printStackTrace();
		} finally {
//			kafkaProducer.close();
		}
	}
}
