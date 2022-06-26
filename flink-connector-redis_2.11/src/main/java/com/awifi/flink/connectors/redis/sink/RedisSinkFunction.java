package com.awifi.flink.connectors.redis.sink;

import com.awifi.flink.connectors.redis.config.FlinkRedisConfigBase;
import com.awifi.flink.connectors.redis.client.RedisCommandsClient;
import com.awifi.flink.connectors.redis.client.RedisCommandsContainerBuilder;
import com.awifi.flink.connectors.redis.mapper.RedisDataType;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Objects;

/**
 * @author francis
 * @Title: RedisSinkFunction
 * @Description:
 * @Date 2022-05-06 16:44
 * @since
 */
public class RedisSinkFunction<IN> extends RichSinkFunction<IN> {

    private static final Logger LOG = LoggerFactory.getLogger(RedisSinkFunction.class);

    private FlinkRedisConfigBase flinkRedisConfigBase;

    private RedisCommandsClient redisCommandsClient;

    private RedisSinkProcessFunction redisSinkProcessFunction;


    public RedisSinkFunction(FlinkRedisConfigBase flinkRedisConfigBase, RedisSinkProcessFunction redisSinkProcessFunction) {
        Objects.requireNonNull(flinkRedisConfigBase, "Redis connection pool config should not be null");

        this.flinkRedisConfigBase = flinkRedisConfigBase;
        this.redisSinkProcessFunction = redisSinkProcessFunction;
    }

    /**
     * Called when new data arrives to the sink, and forwards it to Redis channel.
     * Depending on the specified Redis data type (see {@link RedisDataType}),
     * a different Redis command will be applied.
     * Available commands are RPUSH, LPUSH, SADD, PUBLISH, SET, SETEX, PFADD, HSET, ZADD.
     *
     * @param input The incoming data
     */
    @Override
    public void invoke(IN input, Context context) throws Exception {
        this.redisSinkProcessFunction.process(input, redisCommandsClient, this.getRuntimeContext());
    }

    @Override
    public void open(Configuration parameters) throws Exception {
        try {
            this.redisCommandsClient = RedisCommandsContainerBuilder.build(this.flinkRedisConfigBase);
            this.redisCommandsClient.open();
        } catch (Exception e) {
            LOG.error("Redis has not been properly initialized: ", e);
            throw e;
        }
    }

    @Override
    public void close() throws Exception {
        if (redisCommandsClient != null) {
            redisCommandsClient.close();
        }
    }
}
