package org.doit.function;

import org.apache.flink.streaming.connectors.redis.common.mapper.RedisCommand;
import org.apache.flink.streaming.connectors.redis.common.mapper.RedisCommandDescription;
import org.apache.flink.streaming.connectors.redis.common.mapper.RedisMapper;
import org.doit.bean.ActivityBeanV1;
//redis中使用keys *查看多有的key
public class RedisActivityBeanMapper implements RedisMapper<ActivityBeanV1> {

    @Override
    public RedisCommandDescription getCommandDescription() {
        return new RedisCommandDescription(RedisCommand.HSET, "act_count");
    }

    @Override
    public String getKeyFromData(ActivityBeanV1 bean) {
        return bean.aid+"_"+bean.eventType+"_"+bean.province;
    }

    @Override
    public String getValueFromData(ActivityBeanV1 bean) {
        return String.valueOf(bean.count);
    }
}
