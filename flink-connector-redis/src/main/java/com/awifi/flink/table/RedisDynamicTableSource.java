package com.awifi.flink.table;

import com.awifi.flink.connectors.redis.config.FlinkLettuceClusterConfig;
import com.awifi.flink.connectors.redis.config.RedisConfiguration;
import com.awifi.flink.connectors.redis.exception.UnsupportedLookupRedisCommandException;
import com.awifi.flink.connectors.redis.predefined.RedisLookupCommand;
import com.awifi.flink.connectors.redis.options.RedisLookupOptions;
import com.awifi.flink.connectors.redis.source.RedisLookupFunction;
import org.apache.commons.lang3.StringUtils;
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

    public RedisDynamicTableSource(ResolvedSchema resolvedSchema, RedisConfiguration redisConfiguration, RedisLookupOptions redisLookupOptions) {
        this.resolvedSchema = resolvedSchema;
        this.redisConfiguration = redisConfiguration;
        this.redisLookupOptions = redisLookupOptions;
    }

    @Override
    public LookupRuntimeProvider getLookupRuntimeProvider(LookupContext lookupContext) {

        FlinkLettuceClusterConfig flinkLettuceClusterConfig = FlinkLettuceClusterConfig.build().nodes(redisConfiguration.getNodes())
                .password(redisConfiguration.getPassword())
                .database(redisConfiguration.getDatabase())
                .timeout(redisConfiguration.getTimeout())
                .maxRedirects(redisConfiguration.getMaxRetries())
                .build();

        RedisLookupCommand redisCommand = validateCommandOption(redisConfiguration.getCommand());

        return TableFunctionProvider.of(new RedisLookupFunction(resolvedSchema, redisLookupOptions, flinkLettuceClusterConfig, redisCommand));
    }

    @Override
    public DynamicTableSource copy() {
        return new RedisDynamicTableSource(resolvedSchema, redisConfiguration, redisLookupOptions);
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
