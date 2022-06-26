package com.awifi.flink.table;

import com.awifi.flink.connectors.redis.config.RedisConfiguration;
import com.awifi.flink.connectors.redis.exception.UnsupportedRedisCommandException;
import com.awifi.flink.connectors.redis.mapper.RedisCommand;
import org.apache.commons.lang3.StringUtils;
import org.apache.flink.configuration.ConfigOption;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.ReadableConfig;
import org.apache.flink.table.catalog.ResolvedSchema;
import org.apache.flink.table.connector.sink.DynamicTableSink;
import org.apache.flink.table.connector.source.DynamicTableSource;
import org.apache.flink.table.factories.DynamicTableSinkFactory;
import org.apache.flink.table.factories.DynamicTableSourceFactory;
import org.apache.flink.table.factories.FactoryUtil;
import org.apache.flink.table.types.DataType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;

/**
 * @author francis
 * @Title: RedisDynamicTableFactory
 * @Description:
 * @Date 2022-05-06 14:28
 * @since
 */
public class RedisDynamicTableFactory implements DynamicTableSourceFactory, DynamicTableSinkFactory {

    private static final Logger LOG = LoggerFactory.getLogger(RedisDynamicTableFactory.class);

    public static final String IDENTIFIER = "redis";

    @Override
    public DynamicTableSink createDynamicTableSink(Context context) {
        FactoryUtil.TableFactoryHelper helper = FactoryUtil.createTableFactoryHelper(this, context);
        ReadableConfig config = helper.getOptions();
        //这里只校验table options是否定义
        helper.validate();
        this.validateConfigOptions(config);
        ResolvedSchema resolvedSchema = context.getCatalogTable().getResolvedSchema();

        DataType dataType = resolvedSchema.toPhysicalRowDataType();
        this.validateFieldDataTypes(dataType);

        final Map<String, String> options = context.getCatalogTable().getOptions();
        LOG.debug("input options {}", options);

        /*Configuration configuration = new Configuration();
        options.forEach(configuration::setString);*/

        RedisConfiguration redisConfiguration = new RedisConfiguration(config);

        return new RedisDynamicTableSink(options, resolvedSchema, redisConfiguration);
    }


    @Override
    public DynamicTableSource createDynamicTableSource(Context context) {
        return null;
    }

    @Override
    public String factoryIdentifier() {
        return IDENTIFIER;
    }

    @Override
    public Set<ConfigOption<?>> requiredOptions() {
        Set<ConfigOption<?>> requiredOptions = new HashSet();
        requiredOptions.add(RedisConnectorOptions.NODES);
        requiredOptions.add(RedisConnectorOptions.PASSWORD);
        requiredOptions.add(RedisConnectorOptions.COMMAND);
        return requiredOptions;
    }

    @Override
    public Set<ConfigOption<?>> optionalOptions() {
        Set<ConfigOption<?>> optionalOptions = new HashSet();
        optionalOptions.add(RedisConnectorOptions.TIMEOUT);
        optionalOptions.add(RedisConnectorOptions.SINK_MAX_RETRIES);
        optionalOptions.add(RedisConnectorOptions.SINK_PARALLELISM);
        optionalOptions.add(RedisConnectorOptions.DATABASE);
        optionalOptions.add(RedisConnectorOptions.LENGTH);
        return optionalOptions;
    }

    private void validateConfigOptions(ReadableConfig config) {
        Optional<String> nodes = config.getOptional(RedisConnectorOptions.NODES);
        Optional<String> command = config.getOptional(RedisConnectorOptions.COMMAND);
        try {
            RedisCommand.valueOf(StringUtils.upperCase(command.get()));
        } catch (IllegalArgumentException e) {
            String msg = "command [" + command.get() + "] is not support";
            if (LOG.isErrorEnabled()) {
                LOG.error(msg);
            }
            throw new UnsupportedRedisCommandException(msg);
        }
    }

    private void validateFieldDataTypes(DataType dataType) {
        final List<DataType> fieldDataTypes = dataType.getChildren();
        if (!Optional.ofNullable(fieldDataTypes).isPresent() || fieldDataTypes.size() < 2) {
            throw new IllegalArgumentException("Table physical column cannot be less than 2 columns");
        }
    }
}
