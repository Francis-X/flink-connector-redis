package com.awifi.flink.table;

import com.awifi.flink.connectors.redis.client.RedisClientProvider;
import com.awifi.flink.connectors.redis.client.RedisClientProviderFactory;
import com.awifi.flink.connectors.redis.config.FlinkLettuceRedisConfig;
import com.awifi.flink.connectors.redis.config.PoolConfig;
import com.awifi.flink.connectors.redis.config.RedisConfiguration;
import com.awifi.flink.connectors.redis.exception.UnsupportedRedisCommandException;
import com.awifi.flink.connectors.redis.predefined.RedisSinkCommand;
import com.awifi.flink.connectors.redis.sink.*;
import com.awifi.flink.util.function.Supplier;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.pool2.impl.GenericObjectPoolConfig;
import org.apache.flink.table.catalog.ResolvedSchema;
import org.apache.flink.table.connector.ChangelogMode;
import org.apache.flink.table.connector.sink.DynamicTableSink;
import org.apache.flink.table.connector.sink.SinkFunctionProvider;
import org.apache.flink.types.RowKind;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;

/**
 * @author francis
 * @Title: RedisDynamicTableSink
 * @Description:
 * @Date 2022-05-06 16:15
 * @since
 */
public class RedisDynamicTableSink implements DynamicTableSink {

    private static final Logger LOG = LoggerFactory.getLogger(RedisDynamicTableSink.class);

    protected Integer parallelism;

    private Map<String, String> properties;
    private final transient ResolvedSchema resolvedSchema;

    private final RedisConfiguration redisConfiguration;

    private final PoolConfig poolConfig;

    public RedisDynamicTableSink(Map<String, String> properties, ResolvedSchema resolvedSchema, RedisConfiguration redisConfiguration, PoolConfig poolConfig) {
        this.properties = properties;
        this.resolvedSchema = resolvedSchema;
        this.redisConfiguration = redisConfiguration;
        this.poolConfig = poolConfig;
        this.parallelism = redisConfiguration.getParallelism();
    }

    @Override
    public ChangelogMode getChangelogMode(ChangelogMode changelogMode) {
        this.validatePrimaryKey(changelogMode);
        return ChangelogMode.newBuilder()
                .addContainedKind(RowKind.INSERT)
                .addContainedKind(RowKind.DELETE)
                .addContainedKind(RowKind.UPDATE_AFTER)
                .build();
    }

    @Override
    public SinkRuntimeProvider getSinkRuntimeProvider(Context context) {

        FlinkLettuceRedisConfig flinkLettuceRedisConfig = FlinkLettuceRedisConfig.builder().nodes(redisConfiguration.getNodes())
                .password(redisConfiguration.getPassword())
                .database(redisConfiguration.getDatabase())
                .timeout(redisConfiguration.getTimeout())
                .maxRedirects(redisConfiguration.getMaxRetries())
                .build();
        final RedisClientProvider redisClientProvider = RedisClientProviderFactory.redisClientProvider(flinkLettuceRedisConfig);

        RedisSinkCommand redisSinkCommand = validateCommandOption(redisConfiguration.getCommand());

        final Supplier<GenericObjectPoolConfig> poolConfigSupplier = RedisClientProvider.defaultGenericObjectPoolConfig(poolConfig);

        return SinkFunctionProvider.of(new RedisSinkFunction(redisClientProvider,
                poolConfigSupplier, new RedisClientProvider.DefaultClientResourcesSupplier(),
                redisSinkCommand,
                redisSinkProcessFunction(redisSinkCommand)), this.parallelism);
    }

    private RedisSinkProcessFunction redisSinkProcessFunction(RedisSinkCommand redisSinkCommand) {
        switch (redisSinkCommand) {
            case HSET:
                return new HashRedisSinkProcessFunction(resolvedSchema, redisConfiguration.getTTL());
            case LTRIM:
                return new LtrimRedisSinkProcessFunction(resolvedSchema, redisConfiguration.getLength(), redisConfiguration.getTTL());
            default:
                return new RowRedisSinkProcessFunction(resolvedSchema, redisConfiguration.getTTL());
        }
    }

    @Override
    public DynamicTableSink copy() {
        return new RedisDynamicTableSink(properties, resolvedSchema, redisConfiguration, poolConfig);
    }


    @Override
    public String asSummaryString() {
        return "Redis table sink";
    }

    private void validatePrimaryKey(ChangelogMode requestedMode) {
    }

    /**
     * sink表操作命令校验
     *
     * @param command
     * @return
     */
    private RedisSinkCommand validateCommandOption(String command) {
        try {
            RedisSinkCommand redisCommand = RedisSinkCommand.valueOf(StringUtils.upperCase(command));
            return redisCommand;
        } catch (IllegalArgumentException e) {
            String msg = "sink command [" + command + "] is not support";
            if (LOG.isErrorEnabled()) {
                LOG.error(msg);
            }
            throw new UnsupportedRedisCommandException(msg);
        }
    }
}
