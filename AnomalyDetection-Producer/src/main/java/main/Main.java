package main;

import clients.CsvReader;
import clients.KafkaOpLogProducer;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;
import com.google.common.collect.ImmutableList;
import models.OpLog;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import java.io.BufferedReader;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.util.Properties;

/**
 * Main Class responsible for getting and validating input parameters
 * and executing kafka producer to send messages
 */
public class Main {
    private static final String SPLITTER = ",";
    private static final String FILE_PATH = "F:/ELTE/Third Semester/Open Source Technologies For Real Time/operational_log.csv";
    private static final String SERVER = "localhost:9092";
    private static final String TOPIC_NAME = "operational-log-data";
    private static final ObjectMapper mapper = new ObjectMapper();

    static {
        mapper.enable(SerializationFeature.INDENT_OUTPUT);
    }

    public static void main(String[] args) throws IOException {

        final File csvFile = new File(FILE_PATH);
        if (!csvFile.exists()) {
            throw new FileNotFoundException("File not found");
        }
        final ImmutableList<OpLog> OpLogDataList = ImmutableList
                .copyOf(
                        new CsvReader(SPLITTER)
                                .loadCsvContentToList(new BufferedReader(new FileReader(csvFile)))
                );
        if(OpLogDataList.size() == 0) {
            System.out.println("No Data Found in File");
            return;
        }
        final Properties props = new Properties();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, SERVER);
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG,
                "org.apache.kafka.common.serialization.StringSerializer");
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG,
                "org.apache.kafka.common.serialization.StringSerializer");
        final KafkaProducer<String, String> producer = new KafkaProducer<>(props);
        final KafkaOpLogProducer kafkaOpLogProducer =
                new KafkaOpLogProducer(
                        producer,
                        TOPIC_NAME);
        OpLogDataList.forEach(log -> {
            try {
            	kafkaOpLogProducer.send(mapper.writeValueAsString(log));
            } catch (JsonProcessingException e) {
                e.printStackTrace();
            } 
            finally {
            	kafkaOpLogProducer.close();
            }
        });
    }
}