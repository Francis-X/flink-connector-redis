package com.awifi.flink.connectors.redis.sink;

import com.awifi.flink.connectors.redis.client.RedisClientProvider;
import com.awifi.flink.connectors.redis.client.RedisCommandsExecutor;
import com.awifi.flink.connectors.redis.predefined.RedisDataType;
import com.awifi.flink.connectors.redis.predefined.RedisSinkCommand;
import com.awifi.flink.util.function.Supplier;
import io.lettuce.core.cluster.api.sync.RedisClusterCommands;
import io.lettuce.core.resource.ClientResources;
import org.apache.commons.pool2.impl.GenericObjectPoolConfig;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * @author francis
 * @Title: RedisSinkFunction
 * @Description:
 * @Date 2022-05-06 16:44
 * @since
 */
public class RedisSinkFunction<IN> extends RichSinkFunction<IN> {

    private static final Logger LOG = LoggerFactory.getLogger(RedisSinkFunction.class);
    private static final long serialVersionUID = 1L;

    private RedisSinkProcessFunction redisSinkProcessFunction;

    private final RedisClientProvider redisClientProvider;

    private final Supplier<GenericObjectPoolConfig> poolConfigSupplier;

    private final Supplier<ClientResources> resourcesSupplier;

    private RedisCommandsExecutor.RedisCommands redisCommands;

    public RedisSinkCommand redisSinkCommand;

    public RedisSinkFunction(RedisClientProvider redisClientProvider,
                             Supplier<GenericObjectPoolConfig> poolConfigSupplier,
                             Supplier<ClientResources> resourcesSupplier,
                             RedisSinkCommand redisSinkCommand,
                             RedisSinkProcessFunction redisSinkProcessFunction) {
        this.redisClientProvider = redisClientProvider;
        this.poolConfigSupplier = poolConfigSupplier;
        this.resourcesSupplier = resourcesSupplier;
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
            RedisClusterCommands redisClientCommands = redisClientProvider.getRedisClientCommands(poolConfigSupplier, resourcesSupplier);
            redisClientCommands.echo("Test");
            RedisCommandsExecutor executor = RedisCommandsExecutor.commandsExecutor(redisClientCommands, redisSinkCommand);
            this.redisCommands = executor.redisCommands();
        } catch (Exception e) {
            LOG.error("Redis has not been properly initialized: ", e);
            throw e;
        }
        //System.out.println(String.format("------------------open：provider:%s,commands:%s", Integer.toHexString(redisClientProvider.hashCode()), Integer.toHexString(redisCommands.hashCode())));
    }


    @Override
    public void close() throws Exception {
        if (redisClientProvider != null) {
            //System.out.println(String.format("------------------close：provider:%s,commands:%s", Integer.toHexString(redisClientProvider.hashCode()), Integer.toHexString(redisCommands.hashCode())));
            redisClientProvider.close();
        }
    }
}
