package org.doit;


import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.doit.bean.ActivityBeanV1;
import org.doit.bean.ActivityBeanV2;
import org.doit.function.DataToActivityBeanFunction;
import org.doit.function.GeoToActivityBeanV2Function;
import org.doit.utils.FlinkUtilsV1;
    /*   DataToActivityBeanFunction类,每條數據的活动id關聯數據庫获取活动名称
        user001,A1,2020-09-23 10:10:10,2,北京市
        user002,A3,2020-09-23 10:10:10,1,上海市
        user003,A2,2020-09-23 10:10:10,2,苏州市
        user002,A3,2020-09-23 10:10:10,1,辽宁市
        user001,A2,2020-09-23 10:10:10,2,北京市
        user002,A2,2020-09-23 10:10:10,1,上海市*/
      /*  GeoToActivityBeanV2Function类 查询高德地图API,根据经纬度关联位置信息
        user001,A1,2020-09-23 10:10:10,2,1,115.908923,39.267291
        user002,A2,2020-09-25 12:13:10,2,1,121.26757,37.49794*/
public class QueryActivityName {
    public static void main(String[] args) throws Exception {
        DataStream<String> lines = FlinkUtilsV1.createKafkaStream(args, new SimpleStringSchema());
       // SingleOutputStreamOperator<ActivityBeanV1> beans = lines.map(new DataToActivityBeanFunction());
        SingleOutputStreamOperator<ActivityBeanV2> beans = lines.map(new GeoToActivityBeanV2Function());
        beans.print();
        FlinkUtilsV1.getEnv().execute();


    }
}
