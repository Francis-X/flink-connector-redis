package com.awifi.flink.table;

import com.awifi.flink.connectors.redis.config.RedisConfiguration;
import com.awifi.flink.connectors.redis.options.RedisLookupOptions;
import org.apache.flink.configuration.ConfigOption;
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
import java.util.regex.Pattern;

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

    /**
     * redis url 规范正则表达式
     * 端口：^([0-9]|[1-9]\d{1,3}|[1-5]\d{4}|6[0-4]\d{4}|65[0-4]\d{2}|655[0-2]\d|6553[0-5])$
     * 域名格式
     */
    private static final String NODES_REGEX = "^((?!-)[A-Za-z0-9-]{1,63}(?<!-)\\.)+[A-Za-z]{2,6}(:[1-9][0-9]*)(,((?!-)[A-Za-z0-9-]{1,63}(?<!-)\\.)+[A-Za-z]{2,6}(:([0-9]|[1-9]\\d{1,3}|[1-5]\\d{4}|6[0-4]\\d{4}|65[0-4]\\d{2}|655[0-2]\\d|6553[0-5])))*$";
    /**
     * ip格式
     */
    private static final String IP_REGEX = "^(((25[0-5]|2[0-4]\\d|[0-1]\\d{2}|[1-9]?\\d)\\.){3}(25[0-5]|2[0-4]\\d|[0-1]\\d{2}|[1-9]?\\d)(:([0-9]|[1-9]\\d{1,3}|[1-5]\\d{4}|6[0-4]\\d{4}|65[0-4]\\d{2}|655[0-2]\\d|6553[0-5]))($|(?!,$),))+$";

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

        RedisConfiguration redisConfiguration = new RedisConfiguration(config);

        return new RedisDynamicTableSink(options, resolvedSchema, redisConfiguration);
    }


    @Override
    public DynamicTableSource createDynamicTableSource(Context context) {
        final FactoryUtil.TableFactoryHelper helper = FactoryUtil.createTableFactoryHelper(this, context);
        //这里只校验table options是否定义
        helper.validate();

        ReadableConfig config = helper.getOptions();
        this.validateConfigOptions(config);

        ResolvedSchema resolvedSchema = context.getCatalogTable().getResolvedSchema();

        this.validateFieldDataTypes(resolvedSchema.toPhysicalRowDataType());
        RedisConfiguration redisConfiguration = new RedisConfiguration(config);

        RedisLookupOptions redisLookupOptions = redisLookupOptions(config);

        return new RedisDynamicTableSource(resolvedSchema, redisConfiguration, redisLookupOptions);
    }

    /**
     * 获取redis 维表option 配置
     *
     * @param config
     * @return
     */
    private RedisLookupOptions redisLookupOptions(ReadableConfig config) {
        RedisLookupOptions redisLookupOptions = RedisLookupOptions.builder()
                .cacheMaxSize(config.get(RedisConnectorOptions.LOOKUP_CACHE_MAX_ROWS))
                .cacheExpireMs(config.get(RedisConnectorOptions.LOOKUP_CACHE_TTL).toMillis())
                .maxRetryTimes(config.get(RedisConnectorOptions.LOOKUP_MAX_RETRIES))
                .build();
        return redisLookupOptions;
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
        optionalOptions.add(RedisConnectorOptions.TTL);

        optionalOptions.add(RedisConnectorOptions.LOOKUP_CACHE_MAX_ROWS);
        optionalOptions.add(RedisConnectorOptions.LOOKUP_CACHE_TTL);
        return optionalOptions;
    }

    /**
     * 校验 redis option
     *
     * @param config
     */
    private void validateConfigOptions(ReadableConfig config) {
        //^((?!-)[A-Za-z0-9-]{1,63}(?<!-)\.)+[A-Za-z]{2,6}(:[1-9][0-9]*)$
        //^((?!-)[A-Za-z0-9-]{1,63}(?<!-)\.)+[A-Za-z]{2,6}(:[1-9][0-9]*)(,((?!-)[A-Za-z0-9-]{1,63}(?<!-)\.)+[A-Za-z]{2,6}(:[1-9][0-9]*))*$
        Optional<String> nodes = config.getOptional(RedisConnectorOptions.NODES);
        final String url = nodes.get();
        if (!Pattern.matches(NODES_REGEX, url) && !Pattern.matches(IP_REGEX, url)) {
            if (LOG.isErrorEnabled()) {
                LOG.error("The current redis nodes is wrong:{}", url);
            }
            throw new IllegalArgumentException("Please check redis nodes option");
        }

    }

    /**
     * 校验字段长度不得少于两个
     *
     * @param dataType
     */
    private void validateFieldDataTypes(DataType dataType) {
        final List<DataType> fieldDataTypes = dataType.getChildren();
        if (!Optional.ofNullable(fieldDataTypes).isPresent() || fieldDataTypes.size() < 2) {
            throw new IllegalArgumentException("Table physical column cannot be less than 2 columns");
        }
    }
}
