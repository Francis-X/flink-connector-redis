package com.awifi.flink.connectors.redis.config;

import io.lettuce.core.RedisURI;
import org.apache.commons.pool2.impl.GenericObjectPoolConfig;

import java.net.InetSocketAddress;
import java.util.Set;
import java.util.stream.Collectors;

/**
 * @author francis
 * @Title: FlinkLettuceCluster
 * @Description:
 * @Date 2022-05-06 17:35
 * @since
 */
public class FlinkLettuceClusterConfig extends FlinkRedisConfigBase {


    private final int maxRedirects;

    private final Set<InetSocketAddress> nodes;

    protected FlinkLettuceClusterConfig(int maxTotal, int maxIdle, int minIdle, int connectionTimeout, String password, int maxRedirects, Set<InetSocketAddress> nodes, int database) {
        super(maxTotal, maxIdle, minIdle, connectionTimeout, password, database);
        this.maxRedirects = maxRedirects;
        this.nodes = nodes;
    }

    protected FlinkLettuceClusterConfig(FlinkLettuceClusterConfig.Builder builder) {
        this(builder.maxTotal, builder.maxIdle, builder.minIdle, builder.timeout, builder.password, builder.maxRedirects, builder.nodes, builder.database);
    }

    public int getMaxRedirects() {
        return maxRedirects;
    }

    public Set<RedisURI> getNodes() {
        Set<RedisURI> redisURIS = nodes.stream().map(node ->
                RedisURI.builder().withHost(node.getHostName()).withPort(node.getPort()).withPassword(this.password.toCharArray()).build()
        ).collect(Collectors.toSet());
        return redisURIS;
    }

    /***
     *
     * @param
     * @return {@link Builder}
     * @throws
     * @author francis
     * @date 2022/5/9 16:42
     */
    public static FlinkLettuceClusterConfig.Builder build() {
        return new FlinkLettuceClusterConfig.Builder();
    }


    public static class Builder {
        private Set<InetSocketAddress> nodes;
        private int timeout = 2000;
        private int maxRedirects = 3;
        private int maxTotal = GenericObjectPoolConfig.DEFAULT_MAX_TOTAL;
        private int maxIdle = GenericObjectPoolConfig.DEFAULT_MAX_IDLE;
        private int minIdle = GenericObjectPoolConfig.DEFAULT_MIN_IDLE;
        private String password;
        private int database;

        /**
         * Sets list of node.
         *
         * @param nodes list of node
         * @return Builder itself
         */
        public Builder nodes(Set<InetSocketAddress> nodes) {
            this.nodes = nodes;
            return this;
        }

        /**
         * Sets socket / connection timeout.
         *
         * @param timeout socket / connection timeout, default value is 2000
         * @return Builder itself
         */
        public Builder timeout(int timeout) {
            this.timeout = timeout;
            return this;
        }

        /**
         * Sets limit of redirection.
         *
         * @param maxRedirects limit of redirection, default value is 5
         * @return Builder itself
         */
        public Builder maxRedirects(int maxRedirects) {
            this.maxRedirects = maxRedirects;
            return this;
        }

        /**
         * Sets value for the {@code maxTotal} configuration attribute
         * for pools to be created with this configuration instance.
         *
         * @param maxTotal maxTotal the maximum number of objects that can be allocated by the pool, default value is 8
         * @return Builder itself
         */
        public Builder maxTotal(int maxTotal) {
            this.maxTotal = maxTotal;
            return this;
        }

        /**
         * Sets value for the {@code maxIdle} configuration attribute
         * for pools to be created with this configuration instance.
         *
         * @param maxIdle the cap on the number of "idle" instances in the pool, default value is 8
         * @return Builder itself
         */
        public Builder maxIdle(int maxIdle) {
            this.maxIdle = maxIdle;
            return this;
        }

        /**
         * Sets value for the {@code minIdle} configuration attribute
         * for pools to be created with this configuration instance.
         *
         * @param minIdle the minimum number of idle objects to maintain in the pool, default value is 0
         * @return Builder itself
         */
        public Builder minIdle(int minIdle) {
            this.minIdle = minIdle;
            return this;
        }

        /**
         * Sets value for the {@code password} configuration attribute
         * for pools to be created with this configuration instance.
         *
         * @param password the password for accessing redis cluster
         * @return Builder itself
         */
        public Builder password(String password) {
            this.password = password;
            return this;
        }

        public Builder database(int database) {
            this.database = database;
            return this;
        }

        public FlinkLettuceClusterConfig build() {
            return new FlinkLettuceClusterConfig(this);
        }
    }

}
