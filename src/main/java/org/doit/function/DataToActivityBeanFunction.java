package org.doit.function;

import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.configuration.Configuration;
import org.doit.bean.ActivityBeanV1;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;

public class DataToActivityBeanFunction extends RichMapFunction<String, ActivityBeanV1> {
private Connection connection;
    @Override
    public void open(Configuration parameters) throws Exception {
         connection = DriverManager.getConnection("jdbc:mysql://localhost:3306/spark?characterEncoding=UTF-8", "root", "Cb-123456");
    }

    @Override
    public void close() throws Exception {
        connection.close();
    }

    @Override
    public ActivityBeanV1 map(String line) throws Exception {
        String[] fields = line.split(",");
        String uid=fields[0];
        String aid=fields[1];
        //根据aid作为查询条件查询出name
        PreparedStatement preparedStatement = connection.prepareStatement("select name from activities where id = ?");
        preparedStatement.setString(1,aid);
        ResultSet resultSet = preparedStatement.executeQuery();
        String name=null;
        while (resultSet.next()){
            name=resultSet.getString(1);
        }
        String time=fields[2];
        int eventType = Integer.parseInt(fields[3]);
        String province=fields[4];
        return ActivityBeanV1.of(uid,aid,name,time,eventType,province);

    }
}
