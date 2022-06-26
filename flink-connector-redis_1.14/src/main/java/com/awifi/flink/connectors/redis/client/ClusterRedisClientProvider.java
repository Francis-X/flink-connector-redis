package com.awifi.flink.connectors.redis.client;

import io.lettuce.core.cluster.api.StatefulRedisClusterConnection;
import io.lettuce.core.cluster.api.sync.RedisClusterCommands;
import org.apache.commons.pool2.impl.GenericObjectPool;

import java.util.Objects;

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


    private transient GenericObjectPool<StatefulRedisClusterConnection<String, ?>> clusterPool;

    private transient RedisClusterCommands commands;


    public ClusterRedisClientProvider(GenericObjectPool clusterPool) throws Exception {
        Objects.requireNonNull(clusterPool, "Redis connection pool can not be null");
        this.clusterPool = clusterPool;
        StatefulRedisClusterConnection<String, ?> connection = this.clusterPool.borrowObject();
        this.commands = connection.sync();
    }

    @Override
    public RedisClusterCommands getRedisClientCommands() {
        return commands;
    }


    @Override
    public void close() throws Exception {
        if (clusterPool != null) {
            clusterPool.close();
        }
    }
}
