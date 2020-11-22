package org.doit.function;

import com.alibaba.druid.pool.DruidDataSource;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.async.ResultFuture;
import org.apache.flink.streaming.api.functions.async.RichAsyncFunction;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Collections;
import java.util.concurrent.*;
import java.util.function.Supplier;

public class AsyncMysqlRequest extends RichAsyncFunction<String, String> {
    private transient DruidDataSource dataSource;
    private transient ExecutorService executorService;

    @Override
    public void asyncInvoke(String id, ResultFuture<String> resultFuture) throws Exception {
        //原来mysql查询是阻塞的，返回结果查询才算结束，而现在开启多线程查询，然后将结果放置在future中
        //  executorService的execute方法没有返回值，submit返回future
        //future中可能有值，可能没值，有值的话进行回调
        Future<String> future = executorService.submit(() -> {
            //redis也可以这样，将查询redis放置线程池中
            return queryFromMsql(id);
        });
        CompletableFuture.supplyAsync(new Supplier<String>() {
            @Override
            public String get() {
                try {
                    return future.get();
                } catch (Exception e) {
                    e.printStackTrace();
                    return null;
                }
            }
        }).thenAccept((String dbResult) -> {
            resultFuture.complete(Collections.singleton(dbResult));
        });
    }

    @Override
    public void open(Configuration parameters) throws Exception {
        executorService = Executors.newFixedThreadPool(20);
       //异步请求，在同一时间点有很多请求访问数据库，此方式来一条数据给一个连接
        dataSource.setDriverClassName("com.mysql.jdbc.Driver");
        dataSource.setUsername("root");
        dataSource.setPassword("Cb-123456");
        dataSource.setUrl("jdbc:mysql://localhost:3306/spark?characterEncoding=UTF-8");
        dataSource.setInitialSize(5);
        dataSource.setMinIdle(10);
        dataSource.setMaxActive(20);
    }

    @Override
    public void close() throws Exception {
        dataSource.close();
        executorService.shutdown();
    }

    private String queryFromMsql(String parm) throws SQLException {
        String sql = "select id,name from t_data where id = ?";
        String result = null;
        Connection connection = null;
        PreparedStatement statement = null;
        ResultSet resultSet = null;
        try {
            connection = dataSource.getConnection();
            statement = connection.prepareStatement(sql);
            statement.setString(1, parm);
            resultSet = statement.executeQuery();
            while (resultSet.next()) {
                result = resultSet.getString("name");
            }
        } catch (SQLException e) {
            e.printStackTrace();
        } finally {
            if (resultSet != null) {
                resultSet.close();
            }
            if (statement != null) {
                statement.close();
            }
            if (connection != null) {
                connection.close();
            }
        }
        if (result != null) {
            //放入缓存中
        }
        return result;
    }
}
