package com.awifi.flink.connectors.redis.source;

import com.awifi.flink.connectors.redis.client.RedisClientProvider;
import com.awifi.flink.connectors.redis.options.RedisLookupOptions;
import com.awifi.flink.connectors.redis.predefined.RedisLookupCommand;
import com.awifi.flink.util.function.Supplier;
import io.lettuce.core.cluster.api.sync.RedisClusterCommands;
import io.lettuce.core.resource.ClientResources;
import org.apache.commons.pool2.impl.GenericObjectPoolConfig;
import org.apache.flink.shaded.guava30.com.google.common.cache.Cache;
import org.apache.flink.shaded.guava30.com.google.common.cache.CacheBuilder;
import org.apache.flink.table.catalog.ResolvedSchema;
import org.apache.flink.table.data.GenericRowData;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.functions.FunctionContext;
import org.apache.flink.table.functions.TableFunction;
import org.apache.flink.util.Preconditions;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

/**
 * @author francis
 * @Title: RedisLookupFunction
 * @Description:
 * @Date 2022-06-08 20:41
 * @since
 */
public class RedisLookupFunction extends TableFunction<RowData> {

    private static final Logger LOG = LoggerFactory.getLogger(RedisLookupFunction.class);

    private static final long serialVersionUID = 1L;

    private final long cacheMaxSize;
    private final long cacheExpireMs;

    private final int maxRetryTimes;

    private final RedisLookupCommand redisCommand;

    private final RedisClientProvider redisClientProvider;

    private final Supplier<GenericObjectPoolConfig> poolConfigSupplier;

    private final Supplier<ClientResources> resourcesSupplier;
    /**
     * {@link  org.apache.flink.table.catalog.Column }对象未实现序列化接口,不能作为成员变量
     */
    private final List<SourceColumn> columns;

    private transient RedisClusterCommands<String, String> commands;

    private transient Cache<RowData, RowData> cache;

    public RedisLookupFunction(ResolvedSchema resolvedSchema,
                               RedisLookupOptions lookupOptions,
                               RedisClientProvider redisClientProvider,
                               Supplier<GenericObjectPoolConfig> poolConfigSupplier,
                               Supplier<ClientResources> resourcesSupplier,
                               RedisLookupCommand redisCommand) {
        Preconditions.checkNotNull(redisClientProvider, "Redis provider should not be null");
        Preconditions.checkNotNull(redisCommand, "Redis command should not be null");
        this.cacheMaxSize = lookupOptions.getCacheMaxSize();
        this.cacheExpireMs = lookupOptions.getCacheExpireMs();
        this.maxRetryTimes = lookupOptions.getMaxRetryTimes();
        this.redisClientProvider = redisClientProvider;
        this.poolConfigSupplier = poolConfigSupplier;
        this.resourcesSupplier = resourcesSupplier;
        this.columns = resolvedSchema.getColumns().stream().map(column -> new SourceColumn(column.getName(), column.getDataType())).collect(Collectors.toList());
        this.redisCommand = redisCommand;
        //System.out.println("-------------111111111111-------------------");
    }

    @Override
    public void open(FunctionContext context) throws Exception {
        this.cache =
                this.cacheMaxSize != -1L && this.cacheExpireMs != -1L
                        ? CacheBuilder.newBuilder().expireAfterWrite(this.cacheExpireMs, TimeUnit.MILLISECONDS).maximumSize(this.cacheMaxSize).build()
                        : null;

        RedisClusterCommands redisClientCommands = redisClientProvider.getRedisClientCommands(poolConfigSupplier, resourcesSupplier);
        redisClientCommands.echo("Test");
        this.commands = redisClientCommands;
        //System.out.println(String.format("------------------open：tableFunction:%s,provider:%s,commands:%s",Integer.toHexString(this.hashCode()), Integer.toHexString(redisClientProvider.hashCode()), Integer.toHexString(commands.hashCode())));
    }


    /**
     * @param keys
     */
    public void eval(Object... keys) {
        GenericRowData keyRow = GenericRowData.of(keys);
        //先尝试从缓存中获取
        RowData rowData;
        if (Optional.ofNullable(this.cache).isPresent() && (rowData = this.cache.getIfPresent(keyRow)) != null) {
            this.collect(rowData);
            return;
        }
        GenericRowData genericRowData = null;
        //System.out.println(String.format("------------------eval：tableFunction:%s,provider:%s,commands:%s",Integer.toHexString(this.hashCode()), Integer.toHexString(redisClientProvider.hashCode()), Integer.toHexString(commands.hashCode())));
        for (int retry = 0; retry <= maxRetryTimes; retry++) {
//            switch (redisCommand) {
//                case GET: {
//                    genericRowData = queryString(keys[0]);
//                    break;
//                }
//                case HGETALL: {
//                    genericRowData = queryHash(keys[0]);
//                    break;
//                }
////                default:
////                    throw new UnsupportedLookupRedisCommandException(String.format("Command [%s] does not support dimension table operations", redisCommand.name().toLowerCase()));
//            }
            try {
                if (redisCommand.equals(RedisLookupCommand.GET)) {
                    genericRowData = queryString(keys[0]);
                }
                if (redisCommand.equals(RedisLookupCommand.HGETALL)) {
                    genericRowData = queryHash(keys[0]);
                }
                break;
            } catch (Exception e) {
                LOG.error(String.format("redis executeCommand error, retry times = %d", retry), e);
                if (retry >= maxRetryTimes) {
                    throw new RuntimeException("Execution of redis Command failed.", e);
                }
            }
            try {
                Thread.sleep(500 * (retry + 1));
            } catch (InterruptedException e1) {
                throw new RuntimeException(e1);
            }
        }
        //放入缓存
        if (Optional.ofNullable(this.cache).isPresent() && Optional.ofNullable(genericRowData).isPresent()) {
            this.cache.put(keyRow, genericRowData);
        }
    }

    /**
     * String 结构查询
     *
     * @param keys
     * @return
     */
    private GenericRowData queryString(Object... keys) {
        GenericRowData genericRowData;
        genericRowData = new GenericRowData(2);
        String value = commands.get(String.valueOf(keys[0]));
        genericRowData.setField(0, keys[0]);
        //StringData 类型转化
        SourceColumn column = columns.get(1);
        genericRowData.setField(1, value == null ? null : column.getRedisDeserialize().deserialize(value));
        this.collect(genericRowData);
        return genericRowData;
    }

    /**
     * hash查询
     *
     * @param keys
     * @return
     */
    private GenericRowData queryHash(Object... keys) {
        GenericRowData genericRowData;
        Map<String, String> map = commands.hgetall(String.valueOf(keys[0]));
        genericRowData = new GenericRowData(columns.size());
        genericRowData.setField(0, keys[0]);
        if (Optional.ofNullable(map).isPresent() && !map.isEmpty()) {
            for (int i = 1; i < columns.size(); i++) {
                SourceColumn column = columns.get(i);
                String name = column.getName();
                String value = map.get(name);
                genericRowData.setField(i, value == null ? null : column.getRedisDeserialize().deserialize(value));
            }
        }
        this.collect(genericRowData);
        return genericRowData;
    }


    @Override
    public void close() throws Exception {
        Throwable e = null;
        try {
            if (cache != null) {
                cache.cleanUp();
                cache = null;
            }
        } catch (Throwable e1) {
            e = e1;
            throw e1;
        } finally {
            if (redisClientProvider != null) {
                //System.out.println(String.format("------------------close：provider:%s,commands:%s", Integer.toHexString(redisClientProvider.hashCode()), Integer.toHexString(commands.hashCode())));
                if (e != null) {
                    try {
                        redisClientProvider.close();
                    } catch (Throwable e2) {
                        e.addSuppressed(e2);
                    }
                } else {
                    redisClientProvider.close();
                }
            }
        }

    }
}

