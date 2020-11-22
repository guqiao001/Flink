package org.doit.state;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.common.restartstrategy.RestartStrategies;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.runtime.state.filesystem.FsStateBackend;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.CheckpointConfig;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

public class MapWithState {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment environment = StreamExecutionEnvironment.getExecutionEnvironment();
        //开启了checkpointing,才会有重启策略
        environment.enableCheckpointing(5000);
        //设置重启策略
        environment.getConfig().setRestartStrategy(RestartStrategies.fixedDelayRestart(3, 300));
        //设置状态数据存储的后端,若flink-conf.yaml中已指定(全局)，那么这里不用指定
         environment.setStateBackend(new FsStateBackend("file:///E:\\software\\BigDataCode\\LU76.12_FlinkClient\\src\\main\\java\\org\\doit\\restart\\checkpoint\\backend"));
        // environment.setStateBackend(new FsStateBackend("hdfs://192.168.100.251:9000/checkpoint/backend" ));
        //程序异常退出或人为cancal掉，不删除checkpoint数据
        environment.getCheckpointConfig().enableExternalizedCheckpoints(CheckpointConfig.ExternalizedCheckpointCleanup.RETAIN_ON_CANCELLATION);
        DataStreamSource<String> lines = environment.socketTextStream("localhost", 8888);
        SingleOutputStreamOperator<Tuple2<String, Integer>> result = lines.map(new MapFunction<String, Tuple2<String, Integer>>() {

            @Override
            public Tuple2<String, Integer> map(String word) throws Exception {
                if (word.startsWith("duan")) {
                    throw new RuntimeException("duan来了，程序挂了");
                }
                return Tuple2.of(word, 1);
            }
        });
        KeyedStream<Tuple2<String, Integer>, Tuple> keyedStream = result.keyBy(0);
        SingleOutputStreamOperator<Tuple2<String, Integer>> summed = keyedStream.map(new RichMapFunction<Tuple2<String, Integer>, Tuple2<String, Integer>>() {
            private transient ValueState<Tuple2<String, Integer>> valueState;

            @Override
            public void open(Configuration parameters) throws Exception {
                ValueStateDescriptor<Tuple2<String, Integer>> descriptor = new ValueStateDescriptor<>("wc-keyed-state",
                        TypeInformation.of(new TypeHint<Tuple2<String, Integer>>() {
                        })
                        // Types.TUPLE(Types.STRING,Types.INT)
                );
                 valueState = getRuntimeContext().getState(descriptor);
            }

            @Override
            public Tuple2<String, Integer> map(Tuple2<String, Integer> value) throws Exception {
                String word = value.f0;
                Integer count = value.f1;
                Tuple2<String, Integer> historyKV = valueState.value();
                //第一次
                if (historyKV != null) {
                    historyKV.f1 += value.f1;
                    valueState.update(historyKV);
                    return historyKV;
                } else {
                    valueState.update(value);
                    return value;
                }
            }
        });
        summed.print();
        environment.execute();

    }
}
