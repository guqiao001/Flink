package org.doit.restart;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.restartstrategy.RestartStrategies;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import java.net.InetAddress;

public class RestartStrategiesDemo {
    public static void main(String[] args) throws Exception {
        System.out.println(">>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>environment前");
        StreamExecutionEnvironment environment = StreamExecutionEnvironment.getExecutionEnvironment();
        System.out.println(">>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>environment后");
        //只有开启了checkpointing，才会有重启策略
        //若environment处(类似spark driver)发生异常，程序就挂了
       // environment.enableCheckpointing(500);
        //默认的重启策略是，固定延迟无限重启Integer.MAX_VALUE且没有设置stateBackend,默认将state放置jobmanager内存中
      //  environment.setRestartStrategy(RestartStrategies.fixedDelayRestart(5,300));
        Thread thread = Thread.currentThread();
        long threadid = thread.getId();
        String threadname = thread.getName();
        System.out.println("-------environment线程id为："+threadid+",线程名称为："+threadname+"--------------");
        DataStreamSource<String> lines = environment.socketTextStream("192.168.100.251", 9999);
        SingleOutputStreamOperator<Tuple2<String, Integer>> result = lines.map(new MapFunction<String, Tuple2<String, Integer>>() {

            @Override
            public Tuple2<String, Integer> map(String line) throws Exception {
                if (line.startsWith("duan")) {
                    throw new RuntimeException("duan来了，程序挂了");
                }
                Thread thread = Thread.currentThread();
                long threadid = thread.getId();
                String threadname = thread.getName();
                System.out.println("-------map线程id为："+threadid+",线程名称为："+threadname+"--------------");
                String[] fields = line.split(" ");
                String word = fields[0];
                String count = fields[1];
                return Tuple2.of(word, Integer.parseInt(count));
            }
        });
        result.keyBy(0).sum(1).print().setParallelism(1);
        environment.execute("RestartStrategiesDemo");


    }
}
