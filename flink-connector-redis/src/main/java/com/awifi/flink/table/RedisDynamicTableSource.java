package com.awifi.flink.table;

import com.awifi.flink.connectors.redis.client.RedisClientProvider;
import com.awifi.flink.connectors.redis.client.RedisClientProviderFactory;
import com.awifi.flink.connectors.redis.config.FlinkLettuceRedisConfig;
import com.awifi.flink.connectors.redis.config.PoolConfig;
import com.awifi.flink.connectors.redis.config.RedisConfiguration;
import com.awifi.flink.connectors.redis.exception.UnsupportedLookupRedisCommandException;
import com.awifi.flink.connectors.redis.options.RedisLookupOptions;
import com.awifi.flink.connectors.redis.predefined.RedisLookupCommand;
import com.awifi.flink.connectors.redis.source.RedisLookupFunction;
import com.awifi.flink.util.function.Supplier;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.pool2.impl.GenericObjectPoolConfig;
import org.apache.flink.table.catalog.ResolvedSchema;
import org.apache.flink.table.connector.source.DynamicTableSource;
import org.apache.flink.table.connector.source.LookupTableSource;
import org.apache.flink.table.connector.source.TableFunctionProvider;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * @author francis
 * @Title: RedisDynamicTableSource
 * @Description:
 * @Date 2022-06-08 20:22
 * @since
 */
public class RedisDynamicTableSource implements LookupTableSource {

    private static final Logger LOG = LoggerFactory.getLogger(RedisDynamicTableSource.class);

    private transient ResolvedSchema resolvedSchema;

    private RedisConfiguration redisConfiguration;

    private RedisLookupOptions redisLookupOptions;

    private PoolConfig poolConfig;

    public RedisDynamicTableSource(ResolvedSchema resolvedSchema, RedisConfiguration redisConfiguration, RedisLookupOptions redisLookupOptions, PoolConfig poolConfig) {
        this.resolvedSchema = resolvedSchema;
        this.redisConfiguration = redisConfiguration;
        this.redisLookupOptions = redisLookupOptions;
        this.poolConfig = poolConfig;
    }

    @Override
    public LookupRuntimeProvider getLookupRuntimeProvider(LookupContext lookupContext) {

        FlinkLettuceRedisConfig flinkLettuceRedisConfig = FlinkLettuceRedisConfig.builder().nodes(redisConfiguration.getNodes())
                .password(redisConfiguration.getPassword())
                .database(redisConfiguration.getDatabase())
                .timeout(redisConfiguration.getTimeout())
                .maxRedirects(redisConfiguration.getMaxRetries())
                .build();

        RedisLookupCommand redisCommand = validateCommandOption(redisConfiguration.getCommand());
        final RedisClientProvider redisClientProvider = RedisClientProviderFactory.redisClientProvider(flinkLettuceRedisConfig);
        final Supplier<GenericObjectPoolConfig> poolConfigSupplier = RedisClientProvider.defaultGenericObjectPoolConfig(poolConfig);

        return TableFunctionProvider.of(new RedisLookupFunction(resolvedSchema, redisLookupOptions,
                redisClientProvider,
                poolConfigSupplier, new RedisClientProvider.DefaultClientResourcesSupplier(),
                redisCommand));
    }

    @Override
    public DynamicTableSource copy() {
        return new RedisDynamicTableSource(resolvedSchema, redisConfiguration, redisLookupOptions, poolConfig);
    }


    @Override
    public String asSummaryString() {
        return "Redis table source";
    }


    /**
     * 维表操作命令校验
     *
     * @param command
     * @return
     */
    private RedisLookupCommand validateCommandOption(String command) {
        try {
            RedisLookupCommand redisCommand = RedisLookupCommand.valueOf(StringUtils.upperCase(command));
            return redisCommand;
        } catch (IllegalArgumentException e) {
            String msg = "lookup command [" + command + "] is not support";
            if (LOG.isErrorEnabled()) {
                LOG.error(msg);
            }
            throw new UnsupportedLookupRedisCommandException(msg);
        }
    }
}
