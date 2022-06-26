package com.awifi.flink.connectors.redis.client;

import io.lettuce.core.api.StatefulRedisConnection;
import io.lettuce.core.cluster.api.sync.RedisClusterCommands;
import org.apache.commons.pool2.impl.GenericObjectPool;

import java.util.Objects;

/**
 * @author francis
 * @Title: Singleton
 * @Description:
 * @Date 2022-06-15 09:57
 * @since
 */
public class SingletonRedisClientProvider implements RedisClientProvider {

    private static final long serialVersionUID = 1L;

    private transient GenericObjectPool<StatefulRedisConnection<String, ?>> pool;

    private transient RedisClusterCommands commands;

    /**
     * @param pool commands instance
     */
    public SingletonRedisClientProvider(GenericObjectPool pool) throws Exception {
        Objects.requireNonNull(pool, "singleton Redis connection pool can not be null");
        this.pool = pool;
        StatefulRedisConnection<String, ?> connection = this.pool.borrowObject();
        this.commands = connection.sync();
    }

    @Override
    public RedisClusterCommands getRedisClientCommands() throws Exception {
        return commands;
    }

    @Override
    public void close() throws Exception {
        if (pool != null) {
            pool.close();
        }
    }
}
