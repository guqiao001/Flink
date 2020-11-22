package org.doit.function;

import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;
import org.doit.bean.ActivityBeanV1;
import scala.Tuple2;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;

public class MysqlSink extends RichSinkFunction<ActivityBeanV1> {
    private transient Connection connection;
    @Override
    public void open(Configuration parameters) throws Exception {

        connection = DriverManager.getConnection("jdbc:mysql://localhost:3306/spark?characterEncoding=UTF-8", "root", "Cb-123456");
    }

    @Override
    public void close() throws Exception {
        connection.close();
    }

    @Override
    public void invoke(ActivityBeanV1 value, Context context) throws Exception {
        //connection.prepareStatement("insert  into t_activity_count(aid,event_type,counts) values (?,?,?) on duplicate key update counts=counts + ?");
        PreparedStatement statement=null;
        try {
            statement = connection.prepareStatement("insert  into t_activity_counts(aid,event_type,counts) values (?,?,?) on duplicate key update counts = ?");
            statement.setString(1,value.aid);
            statement.setInt(2,value.eventType);
            statement.setInt(3,value.count);
            statement.setInt(4,value.count);//若有数据执行update
            statement.executeUpdate();
        } finally {
            if(statement!=null){
                statement.close();
            }
        }



    }


}
