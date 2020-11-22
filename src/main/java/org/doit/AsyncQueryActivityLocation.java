package org.doit;

import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.AsyncDataStream;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.functions.async.ResultFuture;
import org.apache.flink.streaming.api.functions.async.RichAsyncFunction;
import org.doit.bean.ActivityBeanV1;
import org.doit.function.AsyncGeotoActivityBeanFunction;
import org.doit.utils.FlinkUtilsV1;

import java.util.concurrent.TimeUnit;

/*
 * 异步io,flink支持，mysql支持、elasticsearch支持
 * */
public class AsyncQueryActivityLocation {
    public static void main(String[] args) throws Exception {
        DataStream<String> lines = FlinkUtilsV1.createKafkaStream(args, new SimpleStringSchema());
        SingleOutputStreamOperator<ActivityBeanV1> result = AsyncDataStream.unorderedWait(lines, new AsyncGeotoActivityBeanFunction(), 0, TimeUnit.MICROSECONDS, 10);
        result.print();
        FlinkUtilsV1.getEnv().execute();
    }

}
