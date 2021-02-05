package websocket;
import java.io.IOException;
import java.util.Collections;
import java.util.Date;
import java.util.Iterator;
import java.util.Properties;

import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer09;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer09;
import org.apache.flink.streaming.util.serialization.SimpleStringSchema;
import org.apache.flink.util.IterableIterator;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.LongDeserializer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.bson.Document;
import org.json.simple.JSONObject;
import org.json.simple.parser.JSONParser;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.mongodb.BasicDBObject;
import com.mongodb.DB;
import com.mongodb.DBCollection;
import com.mongodb.DBObject;
import com.mongodb.MongoClient;
import com.mongodb.util.JSON;

import websocket.ConsumerEndPoint;

public class DataStore {
/*
	private final static String TOPIC = "relayDCToAppServer";
    private final static String BOOTSTRAP_SERVERS ="localhost:9092";
    private static Consumer<Long, String> consumer = null;
    public static String jsonStringData = "";
    private static void setConsumerObject(){
    	final Properties props = new Properties();
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG,
                                    BOOTSTRAP_SERVERS);
        props.put(ConsumerConfig.GROUP_ID_CONFIG,
                                    "group01");
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG,
                LongDeserializer.class.getName());
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG,
                StringDeserializer.class.getName());
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG,"earliest");
        props.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "false");
        props.put("auto.commit.interval.ms",1000);
        props.put("session.timeout.ms",30000);
        consumer = new KafkaConsumer<>(props);
        consumer.subscribe(Collections.singletonList(TOPIC));
    }
    
	
	  private static void setConsumerData() { 
		  ConsumerRecords<Long, String> consumerRecords=null; 
		  while(true) { 
			  consumerRecords = consumer.poll(100);
			  System.out.println("count:"+consumerRecords.count());
			  consumerRecords.forEach(record -> {
				  System.out.printf("Consumer Record:(%d, %s, %d, %d)\n", 
						  record.key(),
						  record.value(), record.partition(), record.offset()); 
				  jsonStringData = record.value().toString(); 
				  System.out.println(jsonStringData); 
				  try {
					  Thread.sleep(2000); 
				  } catch (InterruptedException e) {
					  // TODO Auto-generated
					  e.printStackTrace(); 
				} 
			  }); 
		} 
	}
	 
    
   /* private String setConsumerData() {
    	setConsumerObject();
    	ConsumerRecords<Long, String> consumerRecords= consumer.poll(100);
    	Date recentDate = null;
    	String recentData = "";
    	Iterator it = consumerRecords.iterator();
    	ObjectMapper map = new ObjectMapper();
    	while(it.hasNext()) {
    		ConsumerRecord<Long, String> record = (ConsumerRecord<Long, String>) it.next();
    		IOTDeviceMaster master;
			try {
				master = map.readValue(record.value(), IOTDeviceMaster.class);
				if(recentDate == null) {
	    			recentDate = master.getDate();
	    			recentData = record.value();
	    		} else {
	    			if(recentDate.compareTo(master.getDate()) < 0) {
	    				recentDate = master.getDate();
	    				recentData = record.value();
	    			}
	    		}
			} catch (IOException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
    		
    	}
    	System.out.println("recentData:"+recentData);
    	return recentData;
    }
    */
    /*public static void main(String[] args) {
		// TODO Auto-generated method stub
		setConsumerObject();
		setConsumerData();
	}
*/
	
	
	public void transform() throws Exception {
		Properties properties = new Properties();
		properties.setProperty("bootstrap.servers", "localhost:9092");
		properties.setProperty("zookeeper.connect", "localhost:2181");
		properties.setProperty("group.id", "group01");
		StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
		env.enableCheckpointing(5000);
		DataStream<String> messageStream = env
				.addSource(new FlinkKafkaConsumer09<>("relayDCToAppServer", new SimpleStringSchema(), properties));
		messageStream.addSink(new MongoDBDriver());
		env.execute();
	}
	public FlinkKafkaProducer09<String> createStringProducer(String topic, String kafkaAddress) {
		return new FlinkKafkaProducer09<>(kafkaAddress, topic, new SimpleStringSchema());
	}
	public static void main(String args[]) throws Exception {
		DataStore consumer = new DataStore();
		consumer.transform();
	}
	
}

class MongoDBDriver extends RichSinkFunction<String>{

	@Override
	public void invoke(String streamValue) throws Exception {
		// TODO Auto-generated method stub
		try {
	        // Get config from the config file.
	        JSONParser parser = new JSONParser();
	        Object configObject = parser.parse(streamValue);
	        JSONObject json = (JSONObject) configObject;
	        MongoClient mongo = new MongoClient("localhost", 27017);
	        DB db = mongo.getDB("IOTDeviceData");
	        DBCollection collection = db.getCollection("IOTDataGenerated");
	        BasicDBObject bson = (BasicDBObject) JSON.parse(streamValue);
	        collection.insert(bson);
	        //System.out.println(bson);
	        DBCollection collectionRecent = db.getCollection("RecentIOTData");
	        DBObject record = collectionRecent.findOne();
	        if (record == null) {
	        	collectionRecent.insert(bson);
	        } else {
	        	String collectnDate = (String) record.get("date");
	        	Double collectnDateDouble = Double.valueOf(collectnDate);
	        	String newDate = (String) json.get("date");
	        	Double newDateDouble = Double.valueOf(newDate);
	        	if(newDateDouble > collectnDateDouble) {
	        		DBObject query = new BasicDBObject();
	        		collectionRecent.remove(query);
	        		collectionRecent.insert(bson);
	        		System.out.println(bson);
	        	}
	        }
	        
	         	
	        mongo.close();
	      } catch (Exception e) {
	        e.printStackTrace();
	      }
	}
	
	
	
}

