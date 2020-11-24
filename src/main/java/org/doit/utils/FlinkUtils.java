package org.doit.utils;

import org.apache.flink.api.common.serialization.DeserializationSchema;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.runtime.state.filesystem.FsStateBackend;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.CheckpointConfig;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;

import java.util.Arrays;
import java.util.List;
import java.util.Properties;


public class FlinkUtils {
    private static StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

    public static <T> DataStream<T> createKafkaStream(ParameterTool parameters, Class<? extends DeserializationSchema<T>> clazz) throws IllegalAccessException, InstantiationException {
        //设置全局的参数
        env.getConfig().setGlobalJobParameters(parameters);
        env.enableCheckpointing(parameters.getLong("checkpoint.interval", 5000), CheckpointingMode.EXACTLY_ONCE);
        env.setStateBackend(new FsStateBackend(""));
        env.getCheckpointConfig().enableExternalizedCheckpoints(CheckpointConfig.ExternalizedCheckpointCleanup.RETAIN_ON_CANCELLATION);
        Properties prop = new Properties();
        prop.setProperty("bootstrap.servers", parameters.getRequired("bootstrap.servers"));//多个的话可以指定
        //若没有记录偏移量，第一次从最开始消费 earliest，记录了则从记录处读取
        prop.setProperty("auto.offset.reset", parameters.get("auto.offset.reset", "earliest"));
        //kafka消费者不自动提交偏移量,flink消费kafka时一定要设置false,好像是不用调用commit方法
        prop.setProperty("enable.auto.commit", parameters.get("enable.auto.commit", "false"));
        prop.setProperty("group.id", parameters.getRequired("group.id"));
        String topics = parameters.getRequired("topics");
        List<String> topicsList = (List<String>) Arrays.asList(topics.split(","));
        FlinkKafkaConsumer<T> kafkaConsumer = new FlinkKafkaConsumer<T>(topicsList, clazz.newInstance(), prop);
        kafkaConsumer.setCommitOffsetsOnCheckpoints(true);
        return env.addSource(kafkaConsumer);
    }

    public static StreamExecutionEnvironment getEnv(){
        return env;
    }


}
