package org.doit.utils;

import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;


import java.util.Properties;


public class FlinkUtilsV1 {
    private static StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

    public static DataStream<String> createKafkaStream(String[] args, SimpleStringSchema stringSchema) {
        String topic = args[0];
        String groupId = args[1];
        String brokerList = args[2];
        Properties prop = new Properties();
        prop.setProperty("bootstrap.servers", brokerList);//多个的话可以指定
        //若没有记录偏移量，第一次从最开始消费 earliest
        prop.setProperty("auto.offset.reset", "latest");
        //kafka消费者不自动提交偏移量
        prop.setProperty("enable.auto.commit", "false");
        prop.setProperty("group.id", groupId);
        FlinkKafkaConsumer kafkaConsumer = new FlinkKafkaConsumer(topic, new SimpleStringSchema(), prop);
        return env.addSource(kafkaConsumer);
    }

    public static StreamExecutionEnvironment getEnv() {
        return env;
    }
}
