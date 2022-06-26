package com.awifi.flink.connectors.redis.client;

import io.lettuce.core.api.StatefulRedisConnection;
import io.lettuce.core.api.sync.RedisCommands;
import org.apache.commons.pool2.impl.GenericObjectPool;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;
import java.util.Objects;

/**
 * @author francis
 * @Title: RedisClient
 * @Description:
 * @Date 2022-05-09 15:45
 * @since
 */
public class RedisSingletonCommandsClient implements RedisCommandsClient<RedisCommands> {
    private static final long serialVersionUID = 1L;

    private static final Logger LOG = LoggerFactory.getLogger(RedisClusterCommandsClient.class);

    private transient GenericObjectPool<StatefulRedisConnection<String, String>> pool;

    private RedisCommands<String, String> commands;

    /**
     * @param pool commands instance
     */
    public RedisSingletonCommandsClient(GenericObjectPool pool) throws Exception {
        Objects.requireNonNull(pool, "Redis  can not be null");
        this.pool = pool;
        StatefulRedisConnection<String, String> connection = this.pool.borrowObject();
        this.commands = connection.sync();
    }

    @Override
    public void open() throws Exception {

        // echo() tries to open a connection and echos back the
        // message passed as argument. Here we use it to monitor
        // if we can communicate with the cluster.

        commands.echo("Test");
    }


    @Override
    public void hset(final long ttl, final String key, final Map<String, String> map) throws Exception {
        try {
            commands.hmset(key, map);
            this.expire(commands, key, ttl);
        } catch (Exception e) {
            if (LOG.isErrorEnabled()) {
                LOG.error("Cannot send Redis message with command HSET {}", map, e.getMessage());
            }
            throw e;
        }
    }


    @Override
    public void rpush(final long ttl, final String key, final String... values) throws Exception {
        try {
            commands.rpush(key, values);
            this.expire(commands, key, ttl);
        } catch (Exception e) {
            if (LOG.isErrorEnabled()) {
                LOG.error("Cannot send Redis message with command RPUSH to list {} error message: {}", key, e.getMessage());
            }
            throw e;
        }
    }

    @Override
    public void ltrim(long ttl, String key, String value, int length) throws Exception {
        try {
//            commands.multi();
            commands.lpush(key, value);
            commands.ltrim(key, 0, length - 1);
//            TransactionResult transactionResult = commands.exec();
            this.expire(commands, key, ttl);
        } catch (Exception e) {
            if (LOG.isErrorEnabled()) {
                LOG.error("Cannot send Redis message with command LTRIM to list {} error message: {}", key, e.getMessage());
            }
            throw e;
        }
    }

    @Override
    public void lpush(final long ttl, final String key, final String... values) throws Exception {
        try {
            commands.lpush(key, values);
            this.expire(commands, key, ttl);
        } catch (Exception e) {
            if (LOG.isErrorEnabled()) {
                LOG.error("Cannot send Redis message with command LPUSH to list {} error message: {}", key, e.getMessage());
            }
            throw e;
        }
    }

    @Override
    public void sadd(final long ttl, final String key, final String... values) throws Exception {
        try {
//            commands.multi();
            Long count = commands.sadd(key, values);
            this.expire(commands, key, ttl);
//            TransactionResult transactionResult = commands.exec();
            LOG.debug("sadd values length:{},real count:{}", values.length, count);
        } catch (Exception e) {
            if (LOG.isErrorEnabled()) {
                LOG.error("Cannot send Redis message with command SADD to set {} error message {}", key, e.getMessage());
            }
            throw e;
        }
    }

    @Override
    public void set(final long ttl, final String key, final String value) throws Exception {
        try {
            commands.set(key, value);
            this.expire(commands, key, ttl);
        } catch (Exception e) {
            if (LOG.isErrorEnabled()) {
                LOG.error("Cannot send Redis message with command SET to key {}  value {} error message {}", key, value, e.getMessage());
            }
            throw e;
        }
    }


//    @Override
//    public void setex(final long ttl,final String key, final String value, final Integer ttl) throws Exception {
//        try {
//            RedisCommands<String, String> commands = getCommands();
//            commands.setex(key, ttl, value);
//        } catch (Exception e) {
//            if (LOG.isErrorEnabled()) {
//                LOG.error("Cannot send Redis message with command SETEX to key {} error message {}", key, e.getMessage());
//            }
//            throw e;
//        }
//    }


    @Override
    public void pfadd(final long ttl, final String key, final String... elements) throws Exception {
        try {
            commands.pfadd(key, elements);
            this.expire(commands, key, ttl);
        } catch (Exception e) {
            if (LOG.isErrorEnabled()) {
                LOG.error("Cannot send Redis message with command PFADD to key {} error message {}", key, e.getMessage());
            }
            throw e;
        }
    }

    @Override
    public void zadd(final long ttl, final String key, final String... scoredValue) throws Exception {
        Object[] scoredValues = validateDouble(scoredValue);
        try {
            commands.zadd(key, scoredValues);
            this.expire(commands, key, ttl);
        } catch (Exception e) {
            if (LOG.isErrorEnabled()) {
                LOG.error("Cannot send Redis message with command ZADD to set {} error message {}",
                        key, e.getMessage());
            }
            throw e;
        }
    }


    @Override
    public void publish(final long ttl, final String channelName, final String message) throws Exception {
        try {
            commands.publish(channelName, message);
            this.expire(commands, channelName, ttl);
        } catch (Exception e) {
            if (LOG.isErrorEnabled()) {
                LOG.error("Cannot send Redis message with command PUBLISH to channel {} error message {}", channelName, e.getMessage());
            }
            throw e;
        }
    }

    @Override
    public boolean expire(RedisCommands commands, String key, long ttl) {
        if (ttl != -1) {
            return commands.expire(key, ttl);
        }
        return false;
    }

    /**
     * Closes the {@link GenericObjectPool}.
     */
    @Override
    public void close() {
        if (pool != null) {
            pool.close();
        }
    }

}
