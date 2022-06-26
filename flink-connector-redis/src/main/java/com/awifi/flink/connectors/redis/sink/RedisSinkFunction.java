package com.awifi.flink.connectors.redis.sink;

import com.awifi.flink.connectors.redis.client.RedisClientProvider;
import com.awifi.flink.connectors.redis.client.RedisClientProviderFactory;
import com.awifi.flink.connectors.redis.client.RedisCommandsExecutor;
import com.awifi.flink.connectors.redis.config.FlinkLettuceClusterConfig;
import com.awifi.flink.connectors.redis.predefined.RedisSinkCommand;
import com.awifi.flink.connectors.redis.predefined.RedisDataType;
import io.lettuce.core.cluster.api.sync.RedisClusterCommands;
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

    private FlinkLettuceClusterConfig flinkLettuceClusterConfig;

    private RedisSinkProcessFunction redisSinkProcessFunction;

    private RedisClientProvider redisClientProvider;

    private RedisCommandsExecutor.RedisCommands redisCommands;

    public RedisSinkCommand redisSinkCommand;

    public RedisSinkFunction(FlinkLettuceClusterConfig flinkLettuceClusterConfig, RedisSinkCommand redisSinkCommand, RedisSinkProcessFunction redisSinkProcessFunction) {
        Objects.requireNonNull(flinkLettuceClusterConfig, "Redis connection  config should not be null");
        this.flinkLettuceClusterConfig = flinkLettuceClusterConfig;
        this.redisSinkCommand = redisSinkCommand;
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
        this.redisSinkProcessFunction.process(input, redisCommands, this.getRuntimeContext());
    }

    @Override
    public void open(Configuration parameters) throws Exception {
        try {
            this.redisClientProvider = RedisClientProviderFactory.redisClientProvider(flinkLettuceClusterConfig);
            RedisClusterCommands redisClientCommands = redisClientProvider.getRedisClientCommands();
            redisClientCommands.echo("Test");
            RedisCommandsExecutor executor = RedisCommandsExecutor.commandsExecutor(redisClientCommands, redisSinkCommand);
            this.redisCommands = executor.redisCommands();
        } catch (Exception e) {
            LOG.error("Redis has not been properly initialized: ", e);
            throw e;
        }
    }


    @Override
    public void close() throws Exception {
        if (redisClientProvider != null) {
            redisClientProvider.close();
        }
    }
}
