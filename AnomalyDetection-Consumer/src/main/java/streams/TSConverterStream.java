package streams;
import com.google.gson.JsonObject;
import com.google.gson.JsonParser;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.kstream.KStream;
import models.OpLog;
import java.util.Date;
import java.text.DateFormat;  
import java.text.SimpleDateFormat;  
import com.google.gson.JsonObject;
import com.google.gson.Gson;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;
import org.apache.kafka.streams.kstream.ValueMapper;
import java.util.TimeZone;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.time.Instant;
import java.time.LocalDate;

public class TSConverterStream {

    private static JsonParser jsonParser = new JsonParser();
    private static Map<String, OpLog> OpLogs = new HashMap<>();

    public static void main(String[] args) {

        // create properties
        Properties properties = new Properties();
        properties.setProperty(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "127.0.0.1:9092");
        properties.setProperty(StreamsConfig.APPLICATION_ID_CONFIG, "TSlogs-streams");
        properties.setProperty(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.StringSerde.class.getName());
        properties.setProperty(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.StringSerde.class.getName());

        // create a topology
        StreamsBuilder streamsBuilder = new StreamsBuilder();

        // input topic
        KStream<String, String> inputTopic = streamsBuilder.stream("operational-log-data");
       
        KStream<String, String> convertedStream = inputTopic.mapValues(
                        new ValueMapper<String, String>() {
                            int i = 0;
                            @Override
                            public String apply(String jsonValue) {
                            	JsonObject jsonObject = jsonParser.parse(jsonValue).getAsJsonObject();
                            	String timestamp = jsonObject.get("eventStartEpoc").getAsString();
                            	
                            //	Date event_start_date = new Date(Long.parseLong(timestamp) * 1000);		//Long.parseLong(timestamp)
                            /*	LocalDateTime ldt = Instant.ofEpochMilli(Long.parseLong(timestamp)).atZone(ZoneId.systemDefault()).toLocalDateTime();
                                DateFormat dateFormat = new SimpleDateFormat("yyyy-MM-dd hh:mm:ss");  
                                TimeZone timeZone = TimeZone.getTimeZone("Europe/Budapest");
                                dateFormat.setTimeZone(timeZone);
                                String strDate = dateFormat.format(ldt); */
                                
                                //convert seconds to milliseconds
                                Date date = new Date(Long.parseLong(timestamp)*1000L); 
                                // format of the date
                                SimpleDateFormat jdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
                                jdf.setTimeZone(TimeZone.getTimeZone("Europe/Budapest"));
                                String strDate = jdf.format(date);
                                
                            	jsonObject.addProperty("eventStartDate", strDate);
                            	jsonObject.addProperty("id", i++);
                                String result = new Gson().toJson(jsonObject);
                                return result;
                            }
                        }
                );
        
        convertedStream.to("converted_operational-log-data");

        // build topology
        KafkaStreams kafkaStreams = new KafkaStreams(streamsBuilder.build(), properties);

        // start our streams application
        kafkaStreams.start();

    }

}