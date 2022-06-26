package com.awifi.flink.connectors.redis.sink;

import com.awifi.flink.connectors.redis.client.RedisCommandsExecutor;
import com.awifi.flink.connectors.redis.format.RedisRowDataConverter;
import com.awifi.flink.connectors.redis.source.SourceColumn;
import com.awifi.flink.util.PredicateUtil;
import org.apache.flink.api.common.functions.RuntimeContext;
import org.apache.flink.table.catalog.ResolvedSchema;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.types.DataType;
import org.apache.flink.table.types.logical.LogicalType;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

/**
 * @author francis
 * @Title: HashRedisSinkProcessFunction
 * @Description:
 * @Date 2022-05-28 23:54
 * @since
 */
public class HashRedisSinkProcessFunction implements RedisSinkProcessFunction<RowData, RedisCommandsExecutor.RedisCommands> {

    private final List<SinkColumn> columns;

    private final long ttl;

    public HashRedisSinkProcessFunction(ResolvedSchema resolvedSchema, long ttl) {
        this.ttl = PredicateUtil.checkArgument(t -> t > 0, ttl, String.format("TTL must be greater than 0, but %s is not", ttl));
        if (resolvedSchema.getColumns().size() <= 1) {
            throw new IllegalArgumentException("Table column cannot be less than 2");
        }
        this.columns = resolvedSchema.getColumns().stream().
                map(column -> new SinkColumn(column.getName(), column.getDataType())).collect(Collectors.toList());
    }

    @Override
    public void process(RowData rowData, RedisCommandsExecutor.RedisCommands commands, RuntimeContext var3) throws Exception {
        int length = rowData.getArity();

        SinkColumn keyColumn = columns.get(0);
        RedisRowDataConverter.RedisSerialize redisSerialize = keyColumn.getRedisSerialize();
        String key = String.valueOf(redisSerialize.format(rowData, 0));
        //hash 需要取字段名称
        Map map = new HashMap<>();
        for (int i = 1; i < length; i++) {
            SinkColumn valueColumn = columns.get(i);
            map.put(valueColumn.getName(), valueColumn.getRedisSerialize().format(rowData, i));
        }
        commands.execute(ttl, key, map);
//        client.hset(ttl, key, map);
    }
}
