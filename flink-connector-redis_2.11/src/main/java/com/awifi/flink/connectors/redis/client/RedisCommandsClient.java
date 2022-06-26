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

import java.io.Closeable;
import java.io.Serializable;
import java.util.Map;

/**
 * The container for all available Redis commands.
 */
public interface RedisCommandsClient<C> extends Serializable, Closeable {

    /**
     * @throws Exception if the instance can not be opened properly
     */
    void open() throws Exception;

    /**
     * Sets field in the hash stored at key to value, with TTL, if needed.
     * Setting expire time to key is optional.
     * If key does not exist, a new key holding a hash is created.
     * If field already exists in the hash, it is overwritten.
     *
     * @param ttl expire time
     * @param key Hash name
     * @param map Hash <field, value>
     */
    void hset(long ttl, String key, Map<String, String> map) throws Exception;


    /**
     * @param ttl
     * @param key
     * @param value
     * @param length
     * @throws Exception
     */
    void ltrim(long ttl, String key, final String value, final int length) throws Exception;

    /**
     * Insert the specified value at the tail of the list stored at key.
     * If key does not exist, it is created as empty list before performing the push operation.
     *
     * @param ttl    expire time
     * @param key    Name of the List
     * @param values Value to be added
     */
    void rpush(long ttl, String key, final String... values) throws Exception;

    /**
     * Insert the specified value at the head of the list stored at key.
     * If key does not exist, it is created as empty list before performing the push operation.
     *
     * @param ttl    expire time
     * @param key    Name of the List
     * @param values Value to be added
     */
    void lpush(long ttl, String key, final String... values) throws Exception;

    /**
     * Add the specified member to the set stored at key.
     * Specified members that are already a member of this set are ignored.
     * If key does not exist, a new set is created before adding the specified members.
     *
     * @param ttl    expire time
     * @param key    Name of the Set
     * @param values Value to be added
     */
    void sadd(long ttl, String key, String... values) throws Exception;

    /**
     * Set key to hold the string value. If key already holds a value, it is overwritten,
     * regardless of its type. Any previous time to live associated with the key is
     * discarded on successful SET operation.
     *
     * @param ttl   expire time
     * @param key   the key name in which value to be set
     * @param value the value
     */
    void set(long ttl, String key, String value) throws Exception;

//    /**
//     * Set key to hold the string value, with a time to live (TTL). If key already holds a value,
//     * it is overwritten, regardless of its type. Any previous time to live associated with the key is
//     * reset on successful SETEX operation.
//     *
//     * @param key   the key name in which value to be set
//     * @param value the value
//     * @param ttl   expire time
//     */
//    void setex(String key, String value, Integer ttl) throws Exception;

    /**
     * Adds all the element arguments to the HyperLogLog data structure
     * stored at the variable name specified as first argument.
     *
     * @param ttl      expire time
     * @param key      The name of the key
     * @param elements the element
     */
    void pfadd(long ttl, String key, String... elements) throws Exception;

    /**
     * Adds the specified member with the specified scores to the sorted set stored at key.
     *
     * @param ttl         expire time
     * @param key         The name of the Sorted Set
     * @param scoredValue element to be added
     */
    void zadd(long ttl, String key, String... scoredValue) throws Exception;


    /**
     * Posts a message to the given channel.
     *
     * @param ttl         expire time
     * @param channelName Name of the channel to which data will be published
     * @param message     the message
     */
    void publish(long ttl, String channelName, String message) throws Exception;


    default boolean expire(C c, String key, long ttl) {
        return false;
    }

    default Object[] validateDouble(String[] scoredValue) {
        int length = scoredValue.length;
        if (length % 2 != 0) {
            String msg = String.format("zadd command score and value come in pairs");
            throw new IllegalArgumentException(msg);
        }

        Object[] scoredAndValues = new Object[length];
        for (int i = 0; i < length; i += 2) {
            try {
                scoredAndValues[i] = Double.parseDouble(scoredValue[i]);
                scoredAndValues[i + 1] = scoredValue[i + 1];
            } catch (Exception e) {
                String msg = String.format("zadd command score must be Double DataType but '%s' is not", scoredValue[i]);
                throw new IllegalArgumentException(msg);
            }
        }
        return scoredAndValues;
    }

}
