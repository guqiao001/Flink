package org.doit.restart;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.restartstrategy.RestartStrategies;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.runtime.state.filesystem.FsStateBackend;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.CheckpointConfig;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

public class StateBackendBemo {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment environment = StreamExecutionEnvironment.getExecutionEnvironment();
        //开启了checkpointing,才会有重启策略
        environment.enableCheckpointing(5000);
        //设置重启策略
        environment.getConfig().setRestartStrategy(RestartStrategies.fixedDelayRestart(3, 300));
        //设置状态数据存储的后端,若flink-conf.yaml中已指定(全局)，那么这里不用指定
       // environment.setStateBackend(new FsStateBackend("file:///E:\\software\\BigDataCode\\LU76.12_FlinkClient\\src\\main\\java\\org\\doit\\restart\\checkpoint\\backend"));
        environment.setStateBackend(new FsStateBackend("hdfs://192.168.100.251:9000/checkpoint/backend" ));
       //程序异常退出或人为cancal掉，不删除checkpoint数据
        environment.getCheckpointConfig().enableExternalizedCheckpoints(CheckpointConfig.ExternalizedCheckpointCleanup.RETAIN_ON_CANCELLATION);
        DataStreamSource<String> lines = environment.socketTextStream("192.168.100.251", 9999);
        SingleOutputStreamOperator<Tuple2<String, Integer>> result = lines.map(new MapFunction<String, Tuple2<String, Integer>>() {

            @Override
            public Tuple2<String, Integer> map(String word) throws Exception {
                if (word.startsWith("duan")) {
                    throw new RuntimeException("duan来了，程序挂了");
                }
                return Tuple2.of(word, 1);
            }
        });
        result.keyBy(0).sum(1).print();
        environment.execute("RestartStrategiesDemo");

    }
}
