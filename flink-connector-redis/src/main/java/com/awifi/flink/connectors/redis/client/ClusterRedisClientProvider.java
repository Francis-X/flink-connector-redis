package com.awifi.flink.connectors.redis.client;

import com.awifi.flink.connectors.redis.config.FlinkLettuceRedisConfig;
import com.awifi.flink.util.function.Supplier;
import io.lettuce.core.RedisURI;
import io.lettuce.core.TimeoutOptions;
import io.lettuce.core.cluster.ClusterClientOptions;
import io.lettuce.core.cluster.RedisClusterClient;
import io.lettuce.core.cluster.api.StatefulRedisClusterConnection;
import io.lettuce.core.cluster.api.sync.RedisClusterCommands;
import io.lettuce.core.resource.ClientResources;
import io.lettuce.core.support.ConnectionPoolSupport;
import org.apache.commons.pool2.impl.GenericObjectPool;
import org.apache.commons.pool2.impl.GenericObjectPoolConfig;

import java.time.Duration;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;

/**
 * The type Cluster redis client provider.
 *
 * @author francis
 * @Title: ClusterRedisClientProvider
 * @Description:
 * @Date 2022 -06-15 09:49
 * @since
 */
public class ClusterRedisClientProvider implements RedisClientProvider {

    private static final long serialVersionUID = 1L;

    private final FlinkLettuceRedisConfig flinkLettuceRedisConfig;

    private final Set<RedisURI> uris;

    private transient StatefulRedisClusterConnection<String, String> connection;

    private transient GenericObjectPool<StatefulRedisClusterConnection<?, ?>> clusterPool;


    public ClusterRedisClientProvider(FlinkLettuceRedisConfig flinkLettuceRedisConfig) {
        Objects.requireNonNull(flinkLettuceRedisConfig, "The cluster redis connection configuration should be null ");
        this.flinkLettuceRedisConfig = flinkLettuceRedisConfig;
        this.uris = flinkLettuceRedisConfig.getNodes();
    }

    @Override
    public RedisClusterCommands getRedisClientCommands() {
        return getRedisClientCommands(new DefaultClientResourcesSupplier());
    }

    @Override
    public RedisClusterCommands getRedisClientCommands(Supplier<ClientResources> clientResourcesSupplier) {
        Objects.requireNonNull(clientResourcesSupplier, "Client Resources config can not be null");
        RedisClusterClient redisClusterClient = RedisClusterClient.create(clientResourcesSupplier.get(), uris);
        redisClusterClient.setOptions(clusterClientOptions());
        this.connection = redisClusterClient.connect();
        return this.connection.sync();
    }

    @Override
    public RedisClusterCommands getRedisClientCommands(Supplier<GenericObjectPoolConfig> poolConfigSupplier, Supplier<ClientResources> clientResourcesSupplier) throws Exception {
        Supplier<ClientResources> resourcesSupplier = Optional.ofNullable(clientResourcesSupplier).orElseGet(() -> new DefaultClientResourcesSupplier());
        if (poolConfigSupplier == null) {
            return getRedisClientCommands(resourcesSupplier);
        }
        RedisClusterClient redisClusterClient = RedisClusterClient.create(resourcesSupplier.get(), uris);
        redisClusterClient.setOptions(clusterClientOptions());
        this.connection = redisClusterClient.connect();
        this.clusterPool = ConnectionPoolSupport.createGenericObjectPool(() -> this.connection, poolConfigSupplier.get());
        return this.clusterPool.borrowObject().sync();
    }

    protected ClusterClientOptions clusterClientOptions() {
        return ClusterClientOptions.builder()
                .autoReconnect(true)
                .maxRedirects(flinkLettuceRedisConfig.getMaxRedirects())
                .timeoutOptions(TimeoutOptions.enabled(Duration.ofMillis(flinkLettuceRedisConfig.getConnectionTimeout())))
                .build();
    }

    @Override
    public void close() throws Exception {
        Throwable t = null;
        try {
            if (clusterPool != null) {
                clusterPool.close();
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
