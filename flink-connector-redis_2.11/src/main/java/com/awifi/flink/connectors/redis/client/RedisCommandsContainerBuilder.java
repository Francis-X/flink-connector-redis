package com.awifi.flink.connectors.redis.client;

import com.awifi.flink.connectors.redis.config.FlinkLettuceClusterConfig;
import com.awifi.flink.connectors.redis.config.FlinkRedisConfigBase;
import io.lettuce.core.ClientOptions;
import io.lettuce.core.RedisClient;
import io.lettuce.core.RedisURI;
import io.lettuce.core.cluster.ClusterClientOptions;
import io.lettuce.core.cluster.ClusterTopologyRefreshOptions;
import io.lettuce.core.cluster.RedisClusterClient;
import io.lettuce.core.resource.ClientResources;
import io.lettuce.core.resource.DefaultClientResources;
import io.lettuce.core.support.ConnectionPoolSupport;
import org.apache.commons.pool2.impl.GenericObjectPool;
import org.apache.commons.pool2.impl.GenericObjectPoolConfig;

import java.time.Duration;
import java.time.temporal.ChronoUnit;
import java.util.Objects;

/**
 * @author francis
 * @Title: RedisCommandsContainerBuilder
 * @Description:
 * @Date 2022-05-06 23:12
 * @since
 */
public class RedisCommandsContainerBuilder {

    public static RedisCommandsClient build(FlinkRedisConfigBase flinkRedisConfigBase) throws Exception {
        if (flinkRedisConfigBase instanceof FlinkLettuceClusterConfig) {
            return RedisCommandsContainerBuilder.build((FlinkLettuceClusterConfig) flinkRedisConfigBase);
        }
        return null;
    }

    public static RedisCommandsClient build(FlinkLettuceClusterConfig flinkLettuceClusterConfig) throws Exception {
        Objects.requireNonNull(flinkLettuceClusterConfig, "Redis cluster config should not be Null");

        if (flinkLettuceClusterConfig.getNodes().size() == 1) {
            return redisSingletonCommandsClient(flinkLettuceClusterConfig);
        }

        GenericObjectPoolConfig poolConfig = genericObjectPoolConfig(flinkLettuceClusterConfig);

        ClientResources clientResources = clientResources();

        //定期更新集群拓扑
        /*ClusterTopologyRefreshOptions refreshOptions = ClusterTopologyRefreshOptions
                .builder()
                //每10分钟更新集群拓扑
                .enablePeriodicRefresh(Duration.of(10, ChronoUnit.MINUTES))
                .build();*/

        ClusterClientOptions clusterClientOptions = ClusterClientOptions.builder()
                .autoReconnect(true)
                .maxRedirects(flinkLettuceClusterConfig.getMaxRedirects())
                /*.topologyRefreshOptions(refreshOptions)*/
                .build();

        RedisClusterClient redisClusterClient = RedisClusterClient.create(clientResources, flinkLettuceClusterConfig.getNodes());
        redisClusterClient.setOptions(clusterClientOptions);

        GenericObjectPool pool = ConnectionPoolSupport
                .createGenericObjectPool(redisClusterClient::connect, poolConfig);
        return new RedisClusterCommandsClient(pool);
    }

    private static RedisCommandsClient redisSingletonCommandsClient(FlinkLettuceClusterConfig flinkLettuceClusterConfig) throws Exception {
        GenericObjectPoolConfig poolConfig = genericObjectPoolConfig(flinkLettuceClusterConfig);

        ClientOptions clientOptions = ClientOptions.builder()
                .autoReconnect(true)
                .build();

        ClientResources clientResources = clientResources();

        RedisURI redisURI = flinkLettuceClusterConfig.getNodes().stream().findFirst().get();
        redisURI.setDatabase(flinkLettuceClusterConfig.getDatabase());
        RedisClient redisClient = RedisClient.create(clientResources, redisURI);
        redisClient.setOptions(clientOptions);

        GenericObjectPool pool = ConnectionPoolSupport
                .createGenericObjectPool(redisClient::connect, poolConfig);
        return new RedisSingletonCommandsClient(pool);
    }

    private static ClientResources clientResources() {
        ClientResources clientResources = DefaultClientResources.builder()
                .ioThreadPoolSize(4)
                .computationThreadPoolSize(4)
                .build();
        return clientResources;
    }

    private static GenericObjectPoolConfig genericObjectPoolConfig(FlinkRedisConfigBase flinkRedisConfigBase) {
        GenericObjectPoolConfig poolConfig = new GenericObjectPoolConfig();
        poolConfig.setMaxTotal(flinkRedisConfigBase.getMaxTotal());
        poolConfig.setMaxIdle(flinkRedisConfigBase.getMaxIdle());
        poolConfig.setMinIdle(flinkRedisConfigBase.getMinIdle());
        return poolConfig;
    }
}
