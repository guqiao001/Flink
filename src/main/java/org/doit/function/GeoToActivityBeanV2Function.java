package org.doit.function;

import com.alibaba.fastjson.JSONObject;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.configuration.Configuration;

import org.apache.http.client.methods.CloseableHttpResponse;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClients;
import org.apache.http.impl.nio.client.CloseableHttpAsyncClient;
import org.apache.http.impl.nio.client.HttpAsyncClients;
import org.apache.http.nio.client.HttpAsyncClient;
import org.apache.http.util.EntityUtils;
import org.doit.bean.ActivityBeanV2;


public class GeoToActivityBeanV2Function extends RichMapFunction<String, ActivityBeanV2> {
    //CloseableHttpClient存在被阻塞，代码同步执行时

    private CloseableHttpClient httpClient;
    private CloseableHttpResponse response;

    @Override
    public void open(Configuration parameters) throws Exception {
        super.open(parameters);
        //代码同步执行，若高德不返回，该task阻塞
        httpClient = HttpClients.createDefault();
        //代码同步执行，若高德返回Future(异步)，不论Future是否为空，该task都不阻塞
       // CloseableHttpAsyncClient httpAsyncClient = HttpAsyncClients.createDefault();
       // final HttpGet request = new HttpGet("http://www.apache.org/");
       // Future<HttpResponse> future = httpAsyncClient.execute(request, null);
    }

    @Override
    public void close() throws Exception {
        super.close();
        httpClient.close();
    }

    @Override
    public ActivityBeanV2 map(String line) throws Exception {
        String[] fields = line.split(",");
        String uid = fields[0];
        String aid = fields[1];
        String time = fields[2];
        int eventType = Integer.parseInt(fields[3]);
        double longitude = Double.valueOf(fields[5]);
        double latitude = Double.valueOf(fields[6]);
        String url = "https://restapi.amap.com/v3/geocode/regeo?key=7e4e2df34cc7f9ccd6ab9cfac411dda9&location=" + longitude + "," + latitude;
        String province=null;
        HttpGet httpGet = new HttpGet(url);
        response = httpClient.execute(httpGet);
        try {
            int statusCode = response.getStatusLine().getStatusCode();
            if (statusCode == 200) {
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
        } finally {
            response.close();
        }


        return ActivityBeanV2.of(uid,aid,"年终",time,eventType,longitude,latitude,province);
    }
}
