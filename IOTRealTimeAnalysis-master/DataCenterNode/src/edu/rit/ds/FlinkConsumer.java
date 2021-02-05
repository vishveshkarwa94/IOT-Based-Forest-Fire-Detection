package edu.rit.ds;

import java.io.File;
import java.io.FileOutputStream;
import java.io.FileWriter;
import java.io.IOException;
import java.io.ObjectOutputStream;
import java.io.Serializable;
import java.util.*;
import java.util.function.Consumer;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer09;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer09;
import org.apache.flink.streaming.util.serialization.JSONDeserializationSchema;
import org.apache.flink.streaming.util.serialization.JSONKeyValueDeserializationSchema;
import org.apache.flink.streaming.util.serialization.KeyedSerializationSchema;
import org.apache.flink.streaming.util.serialization.SimpleStringSchema;
import org.apache.flink.util.Collector;
import org.apache.kafka.common.serialization.Serializer;
import org.apache.sling.commons.json.io.JSONWriter;
import org.json.simple.JSONObject;
import org.json.simple.parser.JSONParser;

import com.fasterxml.jackson.core.JsonFactory;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;

import scala.util.control.Exception.Catch;

public class FlinkConsumer implements Serializable {
	public String arrafy(Map<String,ObjectNode> map) {
		JSONObject json= new JSONObject();
		List<ObjectNode> newList=new ArrayList<>();
		for(Map.Entry<String, ObjectNode> entry: map.entrySet()) {
			newList.add(entry.getValue());
		}
		json.put("iotData", newList);
        json.put("date", String.valueOf(System.currentTimeMillis()));
		return json.toJSONString();
	}
	public void transform() throws Exception {
		Properties properties = new Properties();
		properties.setProperty("bootstrap.servers", "localhost:9092");
		properties.setProperty("zookeeper.connect", "localhost:2181");
		properties.setProperty("group.id", "group01");
		StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
		env.enableCheckpointing(5000);
		DataStream<String> messageStream = env
				.addSource(new FlinkKafkaConsumer09<>("relayMasterToDC",  new SimpleStringSchema(), properties));
		DataStream<String> newData = messageStream.flatMap(new FlatMapFunction<String, String>() {
			Map<String,ObjectNode> map=new HashMap<>();
			public void flatMap(String value, Collector<String> out) throws Exception {
				ObjectMapper mapper = new ObjectMapper();
				try {
					JsonNode rootNode = mapper.readValue(value, JsonNode.class);
					if (rootNode.isObject()) {
						ObjectNode obj = mapper.convertValue(rootNode, ObjectNode.class);
						if (obj.has("temp")) {
							map.put(obj.get("id").toString(), obj);
						}
					}
					//System.out.println("map:" + arrafy(map));

				}
				catch (java.io.IOException ex){
						ex.printStackTrace();
					}
				out.collect(arrafy(map));
			}
		});
		FlinkKafkaProducer09<String> flinkDcToApp = new FlinkKafkaProducer09<>("localhost:9092", "relayDCToAppServer", new SimpleStringSchema());
		newData.addSink(flinkDcToApp);
		//System.out.println(newData.print());
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
interface KafkaInterface {
	public boolean check(String s);
}
class KafkaJsonSerializer implements Serializer {


    @Override
    public void configure(Map map, boolean b) {

    }

    @Override
    public byte[] serialize(String s, Object o) {
        byte[] retVal = null;
        ObjectMapper objectMapper = new ObjectMapper();
        try {
            retVal = objectMapper.writeValueAsBytes(o);
        } catch (Exception e) {
        }
        return retVal;
    }

    @Override
    public void close() {

    }

}