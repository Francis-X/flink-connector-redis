package com.awifi.flink.connectors.redis.client;

import com.awifi.flink.connectors.redis.config.FlinkLettuceRedisConfig;
import com.awifi.flink.util.function.Supplier;
import io.lettuce.core.ClientOptions;
import io.lettuce.core.RedisClient;
import io.lettuce.core.RedisURI;
import io.lettuce.core.TimeoutOptions;
import io.lettuce.core.api.StatefulRedisConnection;
import io.lettuce.core.cluster.api.sync.RedisClusterCommands;
import io.lettuce.core.resource.ClientResources;
import io.lettuce.core.support.ConnectionPoolSupport;
import org.apache.commons.pool2.impl.GenericObjectPool;
import org.apache.commons.pool2.impl.GenericObjectPoolConfig;

import java.time.Duration;
import java.util.Objects;
import java.util.Optional;

/**
 * @author francis
 * @Title: Singleton
 * @Description:
 * @Date 2022-06-15 09:57
 * @since
 */
public class SingletonRedisClientProvider implements RedisClientProvider {

    private static final long serialVersionUID = 1L;

//    private transient RedisClusterCommands commands;

    private final RedisURI redisURI;

    private final FlinkLettuceRedisConfig flinkLettuceRedisConfig;

    private transient StatefulRedisConnection<String, String> connection;

    private transient GenericObjectPool<StatefulRedisConnection<?, ?>> pool;

    public SingletonRedisClientProvider(FlinkLettuceRedisConfig flinkLettuceRedisConfig) {
        Objects.requireNonNull(flinkLettuceRedisConfig, "The single node redis connection configuration should be null ");
        this.flinkLettuceRedisConfig = flinkLettuceRedisConfig;
        this.redisURI = flinkLettuceRedisConfig.getNodes().stream().findFirst().get();
        this.redisURI.setDatabase(flinkLettuceRedisConfig.getDatabase());
    }

    @Override
    public RedisClusterCommands getRedisClientCommands() throws Exception {
        return getRedisClientCommands(new DefaultClientResourcesSupplier());
    }

    @Override
    public RedisClusterCommands getRedisClientCommands(Supplier<ClientResources> clientResourcesSupplier) throws Exception {
        Objects.requireNonNull(clientResourcesSupplier, "Client Resources config can not be null");
        RedisClient redisClient = RedisClient.create(clientResourcesSupplier.get(), redisURI);
        redisClient.setOptions(clientOptions());
        this.connection = redisClient.connect();
        return this.connection.sync();
    }


    @Override
    public RedisClusterCommands getRedisClientCommands(Supplier<GenericObjectPoolConfig> poolConfigSupplier, Supplier<ClientResources> clientResourcesSupplier) throws Exception {
        Supplier<ClientResources> resourcesSupplier = Optional.ofNullable(clientResourcesSupplier).orElseGet(() -> new DefaultClientResourcesSupplier());
        if (poolConfigSupplier == null) {
            return getRedisClientCommands(resourcesSupplier);
        }
        RedisClient redisClient = RedisClient.create(resourcesSupplier.get(), redisURI);
        redisClient.setOptions(clientOptions());
        this.connection = redisClient.connect();
        this.pool = ConnectionPoolSupport.createGenericObjectPool(() -> this.connection, poolConfigSupplier.get());
        return this.pool.borrowObject().sync();
    }

    protected ClientOptions clientOptions() {
        return ClientOptions.builder()
                .autoReconnect(true)
                .timeoutOptions(TimeoutOptions.enabled(Duration.ofMillis(flinkLettuceRedisConfig.getConnectionTimeout())))
                .build();
    }

    @Override
    public void close() throws Exception {
        Throwable t = null;
        try {
            if (pool != null) {
                pool.close();
            }
        } catch (Throwable e) {
            t = e;
            throw e;
        } finally {
            if (connection != null) {
                if (t != null) {
                    try {
                        connection.close();
                    } catch (Throwable e1) {
                        t.addSuppressed(e1);
                    }
                } else {
                    connection.close();
                }
            }
        }
    }

}
