package org.window;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.datastream.WindowedStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;

//分组后再调用TumblingWindow,滑动时窗口内的每个组都被执行
public class TumblingWindow {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        DataStreamSource<String> lines = env.socketTextStream("localhost", 8888);
        SingleOutputStreamOperator<Tuple2<String, Integer>> wordAndCount = lines.map(new MapFunction<String, Tuple2<String, Integer>>() {
            @Override
            public Tuple2<String, Integer> map(String value) throws Exception {
                String[] fields = value.split(" ");
                String word = fields[0];
                int count = Integer.parseInt(fields[1]);
                return Tuple2.of(word, count);
            }
        });
        //先分组，再划分窗口
        KeyedStream<Tuple2<String, Integer>, String> keyedStream = wordAndCount.keyBy(new KeySelector<Tuple2<String, Integer>, String>() {
                    @Override
                    public String getKey(Tuple2<String, Integer> value) throws Exception {
                        return value.f0;
                    }
                });
        WindowedStream<Tuple2<String, Integer>, String, TimeWindow> timeWindow = keyedStream.timeWindow(Time.seconds(5));
        SingleOutputStreamOperator<Tuple2<String, Integer>> sum = timeWindow.sum(1);
        sum.print();
        env.execute();

    }
}
