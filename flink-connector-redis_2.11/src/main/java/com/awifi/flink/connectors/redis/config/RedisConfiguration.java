package com.awifi.flink.connectors.redis.config;

import com.awifi.flink.table.RedisConnectorOptions;
import org.apache.flink.configuration.ReadableConfig;

import java.io.Serializable;
import java.net.InetSocketAddress;
import java.util.HashSet;
import java.util.Set;

/**
 * @author francis
 * @Title: RedisConfiguration
 * @Description:
 * @Date 2022-05-13 11:13
 * @since
 */
public class RedisConfiguration implements Serializable {

    private final static String COMMA_SEPARATOR = ",";

    private static final long serialVersionUID = -8256034742824058560L;

    private ReadableConfig config;

    public RedisConfiguration(ReadableConfig config) {
        this.config = config;
    }

    public Set<InetSocketAddress> getNodes() {
        final String nodesStr = config.get(RedisConnectorOptions.NODES);
        final String[] nodes = nodesStr.split(COMMA_SEPARATOR);

        Set<InetSocketAddress> addresses = new HashSet<>();
        for (String node : nodes) {
            String[] split = node.split(":");
            InetSocketAddress address = InetSocketAddress.createUnresolved(split[0], Integer.parseInt(split[1]));
            addresses.add(address);
        }
        return addresses;
    }

    public String getPassword() {
        String password = config.get(RedisConnectorOptions.PASSWORD);
        return password;
    }

    public Long getTimeout() {
        return config.getOptional(RedisConnectorOptions.TIMEOUT)
                .orElse(RedisConnectorOptions.TIMEOUT.defaultValue());
    }

    public String getCommand() {
        return config.get(RedisConnectorOptions.COMMAND);
    }

    public Integer getParallelism() {
        return config.getOptional(RedisConnectorOptions.SINK_PARALLELISM)
                .orElse(RedisConnectorOptions.SINK_PARALLELISM.defaultValue());
    }

    public Integer getDatabase() {
        return config.getOptional(RedisConnectorOptions.DATABASE)
                .orElse(RedisConnectorOptions.DATABASE.defaultValue());
    }

    public Integer getMaxRetries() {
        return config.getOptional(RedisConnectorOptions.SINK_MAX_RETRIES)
                .orElse(RedisConnectorOptions.SINK_MAX_RETRIES.defaultValue());
    }

    public Integer getLength() {
        return config.getOptional(RedisConnectorOptions.LENGTH)
                .orElse(RedisConnectorOptions.LENGTH.defaultValue());
    }

}
