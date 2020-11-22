package org.doit.restart;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.CheckpointConfig;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import org.apache.flink.util.Collector;

import java.util.Properties;

/*
* 目的：观察operatorstate和keyedstate
* kafka消费者消费数据记录偏移量，消费者对应subtask使用operatorstate记录偏移量
* keyby之后，进行聚合操作，进行历史数据集累加，这些subtask使用累加分组后的历史就是keyedstate
*
* */
public class OperatorStateAndKeyedStateDemo {
    public static void main(String[] args) throws Exception {
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.enableCheckpointing(5000);
        env.getCheckpointConfig().enableExternalizedCheckpoints(CheckpointConfig.ExternalizedCheckpointCleanup.RETAIN_ON_CANCELLATION);
        //为了实现EXACTLY_ONCE,必须要记录偏移量，为了保证程序出现问题可以继续累加，
        //要记录分组聚合的中间结果
        env.getCheckpointConfig().setCheckpointingMode(CheckpointingMode.EXACTLY_ONCE);
        Properties properties = new Properties();
        properties.setProperty("bootstrap.servers", "192.168.100.251:9092");
        properties.setProperty("group.id", "test");
        //kafka的消费者不自动提交偏移量，而是交给flink通过checkpointing管理偏移量
        properties.setProperty("enable.auto.commit", "false");
        FlinkKafkaConsumer<String> myConsumer = new FlinkKafkaConsumer<>(
                java.util.regex.Pattern.compile("test-topic-[0-9]"),
                new SimpleStringSchema(),
                properties);
        DataStream<String> lines = env.addSource(myConsumer);
        SingleOutputStreamOperator<String> word = lines.flatMap(new FlatMapFunction<String, String>() {

            @Override
            public void flatMap(String line, Collector<String> out) throws Exception {
                String[] words = line.split(" ");
                for (String word : words) {
                    out.collect(word);
                }
            }
        });
        SingleOutputStreamOperator<Tuple2<String, Integer>> wordAndOne = word.map(new MapFunction<String, Tuple2<String, Integer>>() {
            @Override
            public Tuple2<String, Integer> map(String word) throws Exception {
                return Tuple2.of(word, 1);
            }
        });
        SingleOutputStreamOperator<Tuple2<String, Integer>> sumed = wordAndOne.keyBy(0).sum(1);
        sumed.print();
        env.execute();
    }








}
