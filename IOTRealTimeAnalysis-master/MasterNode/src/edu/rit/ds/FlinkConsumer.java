package edu.rit.ds;

import java.util.Properties;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer09;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer09;
import org.apache.flink.streaming.util.serialization.SimpleStringSchema;

public class FlinkConsumer {

	public void transform() throws Exception {
		Properties properties = new Properties();
		properties.setProperty("bootstrap.servers", "localhost:9092");
		properties.setProperty("zookeeper.connect", "localhost:2181");
		properties.setProperty("group.id", "group01");
		StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
		env.enableCheckpointing(5000);
		DataStream<String> messageStream = env
				.addSource(new FlinkKafkaConsumer09<>("generateLoad", new SimpleStringSchema(), properties));
		messageStream.print();
		FlinkKafkaProducer09<String> flinkKafkaProducer = createStringProducer("relayMasterToDC", "localhost:9092");
		//System.out.println(flinkKafkaProducer);
		messageStream.addSink(flinkKafkaProducer);
		env.execute();
	}
	public FlinkKafkaProducer09<String> createStringProducer(String topic, String kafkaAddress) {
		return new FlinkKafkaProducer09<>(kafkaAddress, topic, new SimpleStringSchema());
	}
	public static void main(String args[]) throws Exception {
		FlinkConsumer consumer = new FlinkConsumer();
		consumer.transform();
	}

}
