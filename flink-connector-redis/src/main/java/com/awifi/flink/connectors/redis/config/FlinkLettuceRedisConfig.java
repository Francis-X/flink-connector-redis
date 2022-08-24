package com.awifi.flink.connectors.redis.config;

import io.lettuce.core.RedisURI;

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
public class FlinkLettuceRedisConfig extends FlinkRedisConfigBase {

    private static final long serialVersionUID = 1L;

    private final int maxRedirects;

    private final Set<InetSocketAddress> nodes;

    protected FlinkLettuceRedisConfig(long connectionTimeout, String password, int maxRedirects, Set<InetSocketAddress> nodes, int database) {
        super(connectionTimeout, password, database);
        this.maxRedirects = maxRedirects;
        this.nodes = nodes;
    }

    protected FlinkLettuceRedisConfig(Builder builder) {
        this(builder.timeout, builder.password, builder.maxRedirects, builder.nodes, builder.database);
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
    public static Builder builder() {
        return new Builder();
    }


    public static class Builder {
        private Set<InetSocketAddress> nodes;
        private long timeout = 2000;
        private int maxRedirects = 3;
        private String password;
        private int database;


        public Builder nodes(Set<InetSocketAddress> nodes) {
            this.nodes = nodes;
            return this;
        }


        public Builder timeout(long timeout) {
            this.timeout = timeout;
            return this;
        }

        public Builder maxRedirects(int maxRedirects) {
            this.maxRedirects = maxRedirects;
            return this;
        }


        public Builder password(String password) {
            this.password = password;
            return this;
        }

        public Builder database(int database) {
            this.database = database;
            return this;
        }

        public FlinkLettuceRedisConfig build() {
            return new FlinkLettuceRedisConfig(this);
        }
    }

}
