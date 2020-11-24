package org.doit;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.doit.function.MyRedisSink;
import org.doit.utils.FlinkUtils;

import java.io.IOException;

public class FlinkKafkaToRedis {

    public static void main(String[] args) throws Exception {
/*
工具类：方式一
        ParameterTool parameterTool = ParameterTool.fromArgs(args);
        String groupid = parameterTool.get("group.id","group01");
        String topics = parameterTool.getRequired("topics"); //必须传如--topics test1,test2

*/

/*      工具类：方式二
       ParameterTool parameterTool = ParameterTool.fromPropertiesFile(args[0]);
        String groupid = parameterTool.get("group.id","group01");
        String topics = parameterTool.getRequired("topics");*/

        ParameterTool parameters = ParameterTool.fromPropertiesFile(args[0]);
        DataStream<String> lines = FlinkUtils.createKafkaStream(parameters, SimpleStringSchema.class);
        SingleOutputStreamOperator<Tuple2<String, Integer>> wordAndCount = lines.map(new MapFunction<String, Tuple2<String, Integer>>() {
            @Override
            public Tuple2<String, Integer> map(String value) throws Exception {
                String[] fields = value.split(" ");
                String word = fields[0];
                int count = Integer.parseInt(fields[1]);
                return Tuple2.of(word, count);
            }
        });
        KeyedStream<Tuple2<String, Integer>, String> keyedStream = wordAndCount.keyBy(new KeySelector<Tuple2<String, Integer>, String>() {
            @Override
            public String getKey(Tuple2<String, Integer> value) throws Exception {
                return value.f0;
            }
        });
        SingleOutputStreamOperator<Tuple2<String, Integer>> summed = keyedStream.sum(1);
        summed.map(new MapFunction<Tuple2<String, Integer>, Tuple3<String, String, String>>() {
            @Override
            public Tuple3<String, String, String> map(Tuple2<String, Integer> value) throws Exception {
                return Tuple3.of("wordcount", value.f0, value.f1.toString());
            }
        }).addSink(new MyRedisSink());
        FlinkUtils.getEnv().execute();
    }


}
