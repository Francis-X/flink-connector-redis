/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.awifi.flink.connectors.redis.client;

import io.lettuce.core.cluster.api.StatefulRedisClusterConnection;
import io.lettuce.core.cluster.api.sync.RedisClusterCommands;
import org.apache.commons.pool2.impl.GenericObjectPool;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;
import java.util.Objects;

/**
 * Redis command container if we want to connect to a Redis cluster.
 */
public class RedisClusterCommandsClient implements RedisCommandsClient<RedisClusterCommands> {

    private static final long serialVersionUID = 1L;

    private static final Logger LOG = LoggerFactory.getLogger(RedisClusterCommandsClient.class);

    private GenericObjectPool<StatefulRedisClusterConnection<String, String>> clusterPool;

    private RedisClusterCommands<String, String> commands;


    /**
     * @param clusterPool commands instance
     */
    public RedisClusterCommandsClient(GenericObjectPool clusterPool) throws Exception {
        Objects.requireNonNull(clusterPool, "Redis  can not be null");
        this.clusterPool = clusterPool;
        StatefulRedisClusterConnection<String, String> connection = this.clusterPool.borrowObject();
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
    public void rpush(long ttl, final String key, final String... values) throws Exception {
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
            commands.lpush(key, value);
            commands.ltrim(key, 0, length - 1);
            this.expire(commands, key, ttl);
        } catch (Exception e) {
            if (LOG.isErrorEnabled()) {
                LOG.error("Cannot send Redis message with command ltrim to list {} error message: {}", key, e.getMessage());
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
            Long count = commands.sadd(key, values);
            this.expire(commands, key, ttl);
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
            commands.setex(key, ttl, value);
        } catch (Exception e) {
            if (LOG.isErrorEnabled()) {
                LOG.error("Cannot send Redis message with command SET to key {}  value {} error message {}", key, value, e.getMessage());
            }
            throw e;
        }
    }

//    @Override
//    public void setex(final String key, final String value, final Integer ttl) throws Exception {
//        try {
//            RedisClusterCommands<String, String> commands = commands;
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
    public boolean expire(RedisClusterCommands commands, String key, long ttl) {
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
        if (clusterPool != null) {
            clusterPool.close();
        }
    }
}
