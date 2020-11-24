package org.doit.source;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.restartstrategy.RestartStrategies;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.runtime.executiongraph.restart.RestartStrategy;
import org.apache.flink.runtime.state.filesystem.FsStateBackend;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.CheckpointConfig;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

public class OperatorStateDemo {
    public static void main(String[] args) throws Exception {
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.enableCheckpointing(5000);
        env.setParallelism(2);
        env.setRestartStrategy(RestartStrategies.fixedDelayRestart(3,5000));
        env.setStateBackend(new FsStateBackend("file:///E:/software/BigDataCode/LU76.12_FlinkClient/src/main/java/org/doit/source/checkpoint/backend"));
        env.getCheckpointConfig().enableExternalizedCheckpoints(CheckpointConfig.ExternalizedCheckpointCleanup.RETAIN_ON_CANCELLATION);
        DataStreamSource<String> stream = env.socketTextStream("192.168.100.251", 8888);
        //socketTextStream用于控制何时出异常,出异常之后文件又被重读一遍
        stream.map(new MapFunction<String, String>() {
            @Override
            public String map(String value) throws Exception {
                if(value.startsWith("duan")){
                    System.out.println(1 / 0);
                }
                return value;
            }
        }).print();

        DataStreamSource<Tuple2<String, String>> source = env.addSource(new MyParFileSource("E:\\software\\BigDataCode\\LU76.12_FlinkClient\\src\\main\\java\\org\\doit\\source\\input"));
        source.print();
        env.execute();

    }
}
