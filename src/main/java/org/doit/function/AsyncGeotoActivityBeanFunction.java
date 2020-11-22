package org.doit.function;

import com.alibaba.fastjson.JSONObject;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.async.ResultFuture;
import org.apache.flink.streaming.api.functions.async.RichAsyncFunction;
import org.apache.http.HttpResponse;
import org.apache.http.client.config.RequestConfig;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.impl.nio.client.CloseableHttpAsyncClient;
import org.apache.http.impl.nio.client.HttpAsyncClients;
import org.apache.http.util.EntityUtils;
import org.doit.bean.ActivityBeanV1;

import java.util.Collections;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Future;
import java.util.function.Supplier;

public class AsyncGeotoActivityBeanFunction extends RichAsyncFunction<String, ActivityBeanV1> {
    private transient CloseableHttpAsyncClient asyncClient;

    @Override
    public void open(Configuration parameters) throws Exception {
        super.open(parameters);
        //初始化异步的HttpClient
        RequestConfig requestConfig = RequestConfig.custom().setSocketTimeout(3000).setConnectTimeout(3000).build();
        asyncClient = HttpAsyncClients.custom().setMaxConnTotal(20).setDefaultRequestConfig(requestConfig).build();
        asyncClient.start();
    }

    @Override
    public void close() throws Exception {
        super.close();
        asyncClient.close();
    }

    @Override
    public void asyncInvoke(String line, ResultFuture<ActivityBeanV1> resultFuture) throws Exception {
        String[] fields = line.split(",");
        String uid = fields[0];
        String aid = fields[1];
        String time = fields[2];
        int eventType = Integer.parseInt(fields[3]);
        double longitude = Double.valueOf(fields[5]);
        double latitude = Double.valueOf(fields[6]);
        String url = "https://restapi.amap.com/v3/geocode/regeo?key=7e4e2df34cc7f9ccd6ab9cfac411dda9&location=" + longitude + "," + latitude;
        HttpGet httpGet = new HttpGet(url);
        Future<HttpResponse> future = asyncClient.execute(httpGet, null);
        CompletableFuture.supplyAsync(new Supplier<String>() {
            @Override
            public String get() {
                try {
                    HttpResponse response = future.get();
                    String province = null;
                    if (response.getStatusLine().getStatusCode() == 200) {
                        //获取请求的json字符串
                        String result = EntityUtils.toString(response.getEntity());
                        System.out.println(result);
                        JSONObject jsonObject = com.alibaba.fastjson.JSON.parseObject(result);
                        JSONObject regeocode = jsonObject.getJSONObject("regeocode");
                        if (regeocode != null && !regeocode.isEmpty()) {
                            JSONObject address = regeocode.getJSONObject("addressComponent");
                            province = address.getString("province");
                            // String city = address.getString("city");
                            // String businessAreas = address.getString("businessAreas");
                        }
                    }
                    return province;
                } catch (Exception e) {
                    e.printStackTrace();
                    return null;
                }
            }
        }).thenAccept((String province) -> {
            resultFuture.complete(Collections.singleton(ActivityBeanV1.of(uid, aid, "年终大促", time, eventType, province)));
        });


    }


}
