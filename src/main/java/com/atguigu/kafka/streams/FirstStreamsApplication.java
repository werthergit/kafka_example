package com.atguigu.kafka.streams;

import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.utils.Bytes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.kstream.*;
import org.apache.kafka.streams.state.KeyValueStore;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Arrays;
import java.util.Locale;
import java.util.Properties;
import java.util.concurrent.CountDownLatch;

public class FirstStreamsApplication {

    private final static Logger LOG = LoggerFactory.getLogger(FirstStreamsApplication.class);

    //0. define variable
    private final static String APP_ID = "first_kafka_streams_01";
    private final static String BOOTSTRAP_SERVERS="bigdata-kafka-01-ontest.chj.cloud:6667,bigdata-kafka-02-ontest.chj.cloud:6667,bigdata-kafka-03-ontest.chj.cloud:6667";
    private final static String SOURCE_TOPIC = "Topic_Input_002";
    private final static String TARGET_TOPIC = "Topic_Input_005";



    public static void main(final String[] args) throws Exception {


        //1. create config
        Properties properties = new Properties();
        properties.put(StreamsConfig.APPLICATION_ID_CONFIG, APP_ID);
        properties.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, BOOTSTRAP_SERVERS);
        //config.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass());
        //config.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass());


        //2.create StreamsBuilder
        StreamsBuilder builder = new StreamsBuilder();
        builder.stream(SOURCE_TOPIC, Consumed.with(Serdes.String(), Serdes.String()).withName("source-processor")
                .withOffsetResetPolicy(Topology.AutoOffsetReset.LATEST))
                .peek((k,v)-> LOG.info("[source] value:{}",v), Named.as("pre-transform-peek"))
                .filter((k,v)->v!=null && v.length()>5,  Named.as("filter-processor"))
                .mapValues(v->v.toUpperCase(), Named.as("map-processor"))
                .peek((k,v)-> LOG.info("[source] value:{}",v), Named.as("post-transform-peek"))
                .to(TARGET_TOPIC, Produced.with(Serdes.String(), Serdes.String()).withName("sink-processor"));

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

        //6.stop
        latch.await();
    }

}
