package com.atguigu.kafka.streams;

import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.Named;
import org.apache.kafka.streams.kstream.Produced;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Properties;
import java.util.concurrent.CountDownLatch;

public class TransactionApplication {

    private final static Logger LOG = LoggerFactory.getLogger(BranchesStreamApplication02.class);

    //0. define variable
    private final static String APP_ID = "kafka_streams_transaction-01";
    private final static String BOOTSTRAP_SERVERS = "bigdata-kafka-01-ontest.chj.cloud:6667,bigdata-kafka-02-ontest.chj.cloud:6667,bigdata-kafka-03-ontest.chj.cloud:6667";
    private final static String SOURCE_TOPIC = "Topic_Input_002";
    private final static String TARGET_TOPIC1 = "Topic_Input_004";
    private final static String TARGET_TOPIC2 = "Topic_Input_006";
    private final static String TARGET_TOPIC3 = "Topic_Input_007";

    public static void main(final String[] args) throws Exception {
        //1. create config
        Properties properties = new Properties();
        properties.put(StreamsConfig.APPLICATION_ID_CONFIG, APP_ID);
        properties.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, BOOTSTRAP_SERVERS);
        // 制定K-V 格式
//        properties.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass()); // Serdes : Data Types and Serialization
//        properties.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass()); // Serdes : Data Types and Serialization


        StreamsBuilder builder = new StreamsBuilder();

        // source
        KStream<String, String> ks0 = builder.stream(SOURCE_TOPIC,
                Consumed.with(Serdes.String(), Serdes.String()).withName("source-processor")
                .withOffsetResetPolicy(Topology.AutoOffsetReset.LATEST));

        // filter
        KStream<String, String> ks1  = ks0.filter((k,v)->v!=null,  Named.as("filter-processor"));


        ks1.filter((k,v)->v.contains("hello"),  Named.as("hello-processor"))
                .peek((k,v)-> LOG.info("1- [source] value:{}",v), Named.as("hello-peek"))
                .to(TARGET_TOPIC1, Produced.with(Serdes.String(), Serdes.String()));

        ks1.filter((k,v)->v.contains("world") || v.contains("hello"),  Named.as("hello-world-processor"))
                .peek((k,v)-> LOG.info("2- [source] value:{}",v), Named.as("hello-world-peek"))
                .to(TARGET_TOPIC2, Produced.with(Serdes.String(), Serdes.String()));


        //3. create topology
        final Topology topology = builder.build();


        //4. create kafka streams
        final KafkaStreams kafkaStreams = new KafkaStreams(topology, properties);

        final CountDownLatch latch = new CountDownLatch(1);

        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            kafkaStreams.close();
            latch.countDown();
            System.out.println("The kafka streams app is getaceful closed.");
        }));

        //5. start
        kafkaStreams.start();

    }

}
