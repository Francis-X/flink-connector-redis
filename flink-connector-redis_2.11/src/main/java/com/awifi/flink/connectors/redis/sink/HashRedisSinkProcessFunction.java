package com.awifi.flink.connectors.redis.sink;

import com.awifi.flink.connectors.redis.client.RedisCommandsClient;
import com.awifi.flink.connectors.redis.format.RowDataToDataFormatter;
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
public class HashRedisSinkProcessFunction implements RedisSinkProcessFunction<RowData, RedisCommandsClient> {

    private final List<String> columnNames;

    private final List<LogicalType> logicalTypes;


    private final static long ttl = 604800;

    public HashRedisSinkProcessFunction(ResolvedSchema resolvedSchema) {
        this.columnNames = resolvedSchema.getColumnNames();
        if (columnNames.size() <= 1) {
            throw new IllegalArgumentException("Table column cannot be less than 2");
        }
        this.logicalTypes = resolvedSchema.getColumnDataTypes().stream().map(DataType::getLogicalType).collect(Collectors.toList());
    }

    @Override
    public void process(RowData rowData, RedisCommandsClient client, RuntimeContext var3) throws Exception {
        int length = rowData.getArity();
        RowDataToDataFormatter.DataFormatString dataFormatString = RowDataToDataFormatter.createDataFormatString(logicalTypes.get(0));
        String key = dataFormatString.format(rowData, 0, logicalTypes.get(0));
        //hash 需要取字段名称
        Map map = new HashMap<>();
        for (int i = 1; i < length; i++) {
            map.put(columnNames.get(i), RowDataToDataFormatter.createDataFormatString(logicalTypes.get(i)).format(rowData, i, logicalTypes.get(i)));
        }
        client.hset(ttl, key, map);
    }
}
