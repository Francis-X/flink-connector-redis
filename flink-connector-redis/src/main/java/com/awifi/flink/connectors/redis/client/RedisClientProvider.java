package com.awifi.flink.connectors.redis.client;

import com.awifi.flink.connectors.redis.config.PoolConfig;
import com.awifi.flink.util.function.Supplier;
import io.lettuce.core.cluster.api.sync.RedisClusterCommands;
import io.lettuce.core.resource.ClientResources;
import io.lettuce.core.resource.DefaultClientResources;
import org.apache.commons.pool2.impl.GenericObjectPoolConfig;

import java.io.Serializable;

/**
 * The interface Redis client provider.
 *
 * @author francis
 * @Title: RedisClientProvider
 * @Description:
 * @Date 2022 -06-15 09:06
 * @since
 */
public interface RedisClientProvider extends AutoCloseable, Serializable {

    /**
     * 获取redis客户端操作命令
     *
     * @return redis client commands
     * @throws Exception the exception
     */
    RedisClusterCommands getRedisClientCommands() throws Exception;

    /**
     * Gets redis client commands.
     *
     * @param resourcesSupplier the client resources supplier
     * @return the redis client commands
     * @throws Exception the exception
     */
    RedisClusterCommands getRedisClientCommands(Supplier<ClientResources> resourcesSupplier) throws Exception;

    /**
     * Gets redis client commands.
     *
     * @param poolConfigSupplier the pool config supplier
     * @param resourcesSupplier  the client resources supplier
     * @return the redis client commands
     * @throws Exception the exception
     */
    RedisClusterCommands getRedisClientCommands(Supplier<GenericObjectPoolConfig> poolConfigSupplier, Supplier<ClientResources> resourcesSupplier) throws Exception;


    static Supplier<GenericObjectPoolConfig> defaultGenericObjectPoolConfig(PoolConfig poolConfig) {
        return () -> {
            final GenericObjectPoolConfig genericObjectPoolConfig = new GenericObjectPoolConfig<>();
            genericObjectPoolConfig.setMaxTotal(poolConfig.getMaxTotal());
            genericObjectPoolConfig.setMaxIdle(poolConfig.getMaxIdle());
            genericObjectPoolConfig.setMinIdle(poolConfig.getMinIdle());
            return genericObjectPoolConfig;
        };
    }

    /**
     * The type Default client resources supplier.
     */
    class DefaultClientResourcesSupplier implements Supplier<ClientResources> {
        @Override
        public ClientResources get() {
            return DefaultClientResources.builder()
                    .ioThreadPoolSize(4)
                    .computationThreadPoolSize(4)
                    .build();
        }
    }

}
