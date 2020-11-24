package org.doit.source;

import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.runtime.state.filesystem.FsStateBackend;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.checkpoint.CheckpointedFunction;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.CheckpointConfig;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;

import java.util.Properties;

public class KafkaSourceV2 {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        //开启checkpoint，同时开启重启策略
        env.enableCheckpointing(5000);
        env.setStateBackend(new FsStateBackend(""));
        env.getCheckpointConfig().enableExternalizedCheckpoints(CheckpointConfig.ExternalizedCheckpointCleanup.RETAIN_ON_CANCELLATION);
        //设置checkpoint模式
        env.getCheckpointConfig().setCheckpointingMode(CheckpointingMode.EXACTLY_ONCE);
        Properties prop = new Properties();
        prop.setProperty("bootstrap.servers", "192.168.100.251:9092");//多个的话可以指定
        //若没有记录偏移量，第一次从最开始消费 earliest，记录了则从记录处读取
        prop.setProperty("auto.offset.reset", "earliest");
        //kafka消费者不自动提交偏移量,flink消费kafka时一定要设置false,好像是不用调用commit方法
        prop.setProperty("enable.auto.commit", "false");
        prop.setProperty("group.id", "group01");
        FlinkKafkaConsumer kafkaConsumer = new FlinkKafkaConsumer("", new SimpleStringSchema(), prop);
        //若开启了checkpoint，offset一定会被保存到checkpoint中
        //flink和kafka的整合中，Kafka中的特殊topic__consumer__offset也记录topic，好处有二：
        // ①可以做监控，比如监控程序处理到那里
        // ②flink程序重启时，没有指定savepoint，那么从kafka中的特殊topic__consumer__offset中记录的消费组的offset处开始消费
        //checkpoint成功后还要向kafka特殊的topic中写入偏移量，默认为true
        //若程序重启时，没有指定savepoint，那么从kafka中的特殊topic__consumer__offset中记录的消费组的offset处开始消费，
        //即是checkpoint中的offset优先于kafka中的特殊topic记录的offset
        kafkaConsumer.setCommitOffsetsOnCheckpoints(true); //若设置为false，则不kafka中记录偏移量，但是会记录在checkpoint中

        DataStreamSource dataSource = env.addSource(kafkaConsumer);

        dataSource.print();
        env.execute();
    }
}
