package com.awifi.flink.table;

import com.awifi.flink.connectors.redis.config.FlinkLettuceClusterConfig;
import com.awifi.flink.connectors.redis.config.RedisConfiguration;
import com.awifi.flink.connectors.redis.mapper.RedisCommand;
import com.awifi.flink.connectors.redis.sink.*;
import org.apache.commons.lang3.StringUtils;
import org.apache.flink.table.catalog.ResolvedSchema;
import org.apache.flink.table.connector.ChangelogMode;
import org.apache.flink.table.connector.sink.DynamicTableSink;
import org.apache.flink.table.connector.sink.SinkFunctionProvider;
import org.apache.flink.types.RowKind;

import java.util.Map;

/**
 * @author francis
 * @Title: RedisDynamicTableSink
 * @Description:
 * @Date 2022-05-06 16:15
 * @since
 */
public class RedisDynamicTableSink implements DynamicTableSink {


    protected Integer parallelism;

    private Map<String, String> properties;
    private transient ResolvedSchema resolvedSchema;

    private RedisConfiguration redisConfiguration;

    public RedisDynamicTableSink(Map<String, String> properties, ResolvedSchema resolvedSchema, RedisConfiguration redisConfiguration) {
        this.properties = properties;
        this.resolvedSchema = resolvedSchema;
        this.redisConfiguration = redisConfiguration;
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

        FlinkLettuceClusterConfig flinkLettuceClusterConfig = FlinkLettuceClusterConfig.build().nodes(redisConfiguration.getNodes())
                .password(redisConfiguration.getPassword())
                .database(redisConfiguration.getDatabase())
                .maxRedirects(redisConfiguration.getMaxRetries())
                .build();
        String command = redisConfiguration.getCommand();

        return SinkFunctionProvider.of(new RedisSinkFunction(flinkLettuceClusterConfig, redisSinkProcessFunction(command.toUpperCase())), this.parallelism);

    }

    private RedisSinkProcessFunction redisSinkProcessFunction(String command) {
        switch (RedisCommand.valueOf(command)) {
            case HSET:
                return new HashRedisSinkProcessFunction(resolvedSchema);
            case LTRIM:
                return new LtrimRedisSinkProcessFunction(resolvedSchema, redisConfiguration.getLength());
            default:
                return new RowRedisSinkProcessFunction(command, resolvedSchema);
        }
    }

    @Override
    public DynamicTableSink copy() {
        return new RedisDynamicTableSink(properties, resolvedSchema, redisConfiguration);
    }


    @Override
    public String asSummaryString() {
        return "Redis table sink";
    }

    private void validatePrimaryKey(ChangelogMode requestedMode) {

    }
}
