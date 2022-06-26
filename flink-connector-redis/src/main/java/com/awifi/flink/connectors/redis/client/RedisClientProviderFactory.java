package com.awifi.flink.connectors.redis.client;

import com.awifi.flink.connectors.redis.config.FlinkLettuceClusterConfig;
import com.awifi.flink.connectors.redis.config.FlinkRedisConfigBase;
import io.lettuce.core.ClientOptions;
import io.lettuce.core.RedisClient;
import io.lettuce.core.RedisURI;
import io.lettuce.core.TimeoutOptions;
import io.lettuce.core.cluster.ClusterClientOptions;
import io.lettuce.core.cluster.RedisClusterClient;
import io.lettuce.core.resource.ClientResources;
import io.lettuce.core.resource.DefaultClientResources;
import io.lettuce.core.support.ConnectionPoolSupport;
import org.apache.commons.pool2.impl.GenericObjectPool;
import org.apache.commons.pool2.impl.GenericObjectPoolConfig;

import java.time.Duration;
import java.util.Objects;

/**
 * @author francis
 * @Title: RedisClientProviderFactory
 * @Description:
 * @Date 2022-06-15 14:33
 * @since
 */
public class RedisClientProviderFactory {

    public static RedisClientProvider redisClientProvider(FlinkLettuceClusterConfig flinkLettuceClusterConfig) throws Exception {
        if (flinkLettuceClusterConfig.getNodes().size() == 1) {
            GenericObjectPoolConfig poolConfig = genericObjectPoolConfig(flinkLettuceClusterConfig);

            ClientOptions clientOptions = ClientOptions.builder()
                    .autoReconnect(true)
                    .timeoutOptions(TimeoutOptions.enabled(Duration.ofMillis(flinkLettuceClusterConfig.getConnectionTimeout())))
                    .build();

            ClientResources clientResources = clientResources();

            RedisURI redisURI = flinkLettuceClusterConfig.getNodes().stream().findFirst().get();
            redisURI.setDatabase(flinkLettuceClusterConfig.getDatabase());
            RedisClient redisClient = RedisClient.create(clientResources, redisURI);
            redisClient.setOptions(clientOptions);

            GenericObjectPool pool = ConnectionPoolSupport
                    .createGenericObjectPool(() -> redisClient.connect(), poolConfig);

            return new SingletonRedisClientProvider(pool);
        }

        Objects.requireNonNull(flinkLettuceClusterConfig, "Redis cluster config should not be Null");
        GenericObjectPoolConfig poolConfig = genericObjectPoolConfig(flinkLettuceClusterConfig);

        ClientResources clientResources = clientResources();

        ClusterClientOptions clusterClientOptions = ClusterClientOptions.builder()
                .autoReconnect(true)
                .maxRedirects(flinkLettuceClusterConfig.getMaxRedirects())
                .timeoutOptions(TimeoutOptions.enabled(Duration.ofMillis(flinkLettuceClusterConfig.getConnectionTimeout())))
                .build();

        RedisClusterClient redisClusterClient = RedisClusterClient.create(clientResources, flinkLettuceClusterConfig.getNodes());
        redisClusterClient.setOptions(clusterClientOptions);
        GenericObjectPool pool = ConnectionPoolSupport
                .createGenericObjectPool(() -> redisClusterClient.connect(), poolConfig);

        return new ClusterRedisClientProvider(pool);
    }


    /**
     * 资源配置
     *
     * @return
     */
    private static ClientResources clientResources() {
        ClientResources clientResources = DefaultClientResources.builder()
                .ioThreadPoolSize(4)
                .computationThreadPoolSize(4)
                .build();
        return clientResources;
    }

    /**
     * commons pool2 连接池
     *
     * @param flinkRedisConfigBase
     * @return
     */
    private static GenericObjectPoolConfig genericObjectPoolConfig(FlinkRedisConfigBase flinkRedisConfigBase) {
        GenericObjectPoolConfig poolConfig = new GenericObjectPoolConfig();
        poolConfig.setMaxTotal(flinkRedisConfigBase.getMaxTotal());
        poolConfig.setMaxIdle(flinkRedisConfigBase.getMaxIdle());
        poolConfig.setMinIdle(flinkRedisConfigBase.getMinIdle());
        return poolConfig;
    }
}
