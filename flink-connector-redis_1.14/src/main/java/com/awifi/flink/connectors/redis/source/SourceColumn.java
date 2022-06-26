package com.awifi.flink.connectors.redis.source;

import com.awifi.flink.connectors.redis.format.RedisRowDataConverter;
import com.awifi.flink.table.Column;
import org.apache.flink.table.types.DataType;

/**
 * @author francis
 * @Title: SourceColumn
 * @Description:
 * @Date 2022-06-17 17:19
 * @since
 */
public class SourceColumn extends Column {

    private static final long serialVersionUID = 1L;

    /**
     * 反序列化器
     */
    private final RedisRowDataConverter.RedisDeserialize redisDeserialize;

    public SourceColumn(String name, DataType dataType) {
        super(name, dataType);
        this.redisDeserialize = RedisRowDataConverter.createRedisDeserialize(dataType.getLogicalType());
    }

    public RedisRowDataConverter.RedisDeserialize getRedisDeserialize() {
        return redisDeserialize;
    }
}
