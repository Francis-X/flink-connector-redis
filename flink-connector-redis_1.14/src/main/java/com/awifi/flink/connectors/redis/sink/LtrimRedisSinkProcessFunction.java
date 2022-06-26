package com.awifi.flink.connectors.redis.sink;

import com.awifi.flink.connectors.redis.client.RedisCommandsExecutor;
import com.awifi.flink.util.PredicateUtil;
import org.apache.flink.api.common.functions.RuntimeContext;
import org.apache.flink.table.catalog.ResolvedSchema;
import org.apache.flink.table.data.RowData;

import java.util.List;
import java.util.stream.Collectors;

/**
 * @author francis
 * @Title: LtrimRedisSinkProcessFunction
 * @Description:
 * @Date 2022-05-31 15:28
 * @since
 */
public class LtrimRedisSinkProcessFunction implements RedisSinkProcessFunction<RowData, RedisCommandsExecutor.RedisCommands> {

    private final List<SinkColumn> columns;

    private final long ttl;

    private final int length;

    public LtrimRedisSinkProcessFunction(ResolvedSchema resolvedSchema, Integer length, long ttl) {
        this.ttl = PredicateUtil.checkArgument(t -> t > 0, ttl, String.format("TTL must be greater than 0, but %s is not", ttl));
        if (resolvedSchema.getColumns().size() <= 1) {
            throw new IllegalArgumentException("Table column cannot be less than 2");
        }
        this.columns = resolvedSchema.getColumns().stream().
                map(column -> new SinkColumn(column.getName(), column.getDataType())).collect(Collectors.toList());

        this.length = length;
    }


    @Override
    public void process(RowData rowData, RedisCommandsExecutor.RedisCommands redisCommands, RuntimeContext var3) throws Exception {

        SinkColumn keyColumn = columns.get(0);
        String key = String.valueOf(keyColumn.getRedisSerialize().format(rowData, 0));

        SinkColumn valueColumn = columns.get(1);
        Object value = valueColumn.getRedisSerialize().format(rowData, 1);
        final Object[] values = new Object[2];
        values[0] = value;
        values[1] = length;
        redisCommands.execute(ttl, key, values);
    }
}
