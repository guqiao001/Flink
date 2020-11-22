package org.doit;

import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.streaming.api.datastream.AsyncDataStream;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.connectors.redis.RedisSink;
import org.apache.flink.streaming.connectors.redis.common.config.FlinkJedisPoolConfig;
import org.doit.bean.ActivityBeanV1;
import org.doit.bean.ActivityBeanV2;
import org.doit.function.AsyncGeotoActivityBeanFunction;
import org.doit.function.GeoToActivityBeanV2Function;
import org.doit.function.MysqlSink;
import org.doit.function.RedisActivityBeanMapper;
import org.doit.utils.FlinkUtilsV1;

import java.util.concurrent.TimeUnit;
/*
   user001,A1,2020-09-23 10:10:10,2,1,115.908923,39.267291
   user002,A2,2020-09-25 12:13:10,2,1,121.26757,37.49794    */
public class ActivityCount {
    public static void main(String[] args) throws Exception {
        DataStream<String> lines = FlinkUtilsV1.createKafkaStream(args, new SimpleStringSchema());
        SingleOutputStreamOperator<ActivityBeanV1> beans = AsyncDataStream.unorderedWait(lines, new AsyncGeotoActivityBeanFunction(), 0, TimeUnit.MICROSECONDS, 10);
        //SingleOutputStreamOperator<ActivityBeanV1> sum = beans.keyBy("aid", "eventType").sum("count");
        //sum.addSink(new MysqlSink());
        SingleOutputStreamOperator<ActivityBeanV1> sum =beans.keyBy("aid","eventType","province").sum("count");
        FlinkJedisPoolConfig conf = new FlinkJedisPoolConfig.Builder().setHost("127.0.0.1").build();
        sum.addSink(new RedisSink<ActivityBeanV1>(conf, new RedisActivityBeanMapper()));
        FlinkUtilsV1.getEnv().execute();
    }
}
