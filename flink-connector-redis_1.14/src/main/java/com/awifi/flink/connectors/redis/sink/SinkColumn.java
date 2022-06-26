package com.awifi.flink.connectors.redis.sink;

import com.awifi.flink.connectors.redis.format.RedisRowDataConverter;
import com.awifi.flink.table.Column;
import org.apache.flink.table.types.DataType;

/**
 * @author francis
 * @Title: SinkColumn
 * @Description:
 * @Date 2022-06-17 17:16
 * @since
 */
public class SinkColumn extends Column {

    private static final long serialVersionUID = 1L;

    /**
     * 序列化器
     */
    private RedisRowDataConverter.RedisSerialize redisSerialize;

    public SinkColumn(String name, DataType dataType) {
        super(name, dataType);
        this.redisSerialize = RedisRowDataConverter.createRedisSerialize(dataType.getLogicalType());
    }

    public RedisRowDataConverter.RedisSerialize getRedisSerialize() {
        return redisSerialize;
    }

}
