package com.atguigu.kafka.streams;

import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.Named;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Properties;
import java.util.concurrent.CountDownLatch;

public class BranchesStreamApplication02 {

    private final static Logger LOG = LoggerFactory.getLogger(BranchesStreamApplication02.class);

    //0. define variable
    private final static String APP_ID = "kafka_streams_Branche_01";
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
        properties.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass()); // Serdes : Data Types and Serialization
        properties.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass()); // Serdes : Data Types and Serialization



        //3. create topology
        final Topology topology = buildTopology();

        //4. create kafka streams
        final KafkaStreams kafkaStreams = new KafkaStreams(topology, properties);

        final CountDownLatch latch = new CountDownLatch(1);

        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            kafkaStreams.close();
            latch.countDown();
            System.out.println("The kafka streams branches app is getaceful closed.");
        }));

        //5. start
        kafkaStreams.start();

        //6.stop
        latch.await();
    }

    private static Topology buildTopology() {
        final StreamsBuilder builder = new StreamsBuilder();

        KStream<String, String>[] branches = builder
                .stream(SOURCE_TOPIC, Consumed.with(Serdes.String(), Serdes.String()))
                .peek((k,v)-> LOG.info("[source] value:{}",v), Named.as("pre-transform-peek"))
                .branch((key, value) -> value.contains("hello"),
                        (key, value) -> value.contains("world"),
                        (key, value) -> true);
        branches[0].to(TARGET_TOPIC1);
        branches[1].to(TARGET_TOPIC2);
        branches[2].to(TARGET_TOPIC3);

        return builder.build();
    }




}
