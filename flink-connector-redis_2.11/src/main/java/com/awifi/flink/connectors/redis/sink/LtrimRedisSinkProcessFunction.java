package com.awifi.flink.connectors.redis.sink;

import com.awifi.flink.connectors.redis.client.RedisCommandsClient;
import com.awifi.flink.connectors.redis.format.RowDataToDataFormatter;
import org.apache.flink.api.common.functions.RuntimeContext;
import org.apache.flink.table.catalog.ResolvedSchema;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.types.DataType;
import org.apache.flink.table.types.logical.LogicalType;

import java.util.List;
import java.util.stream.Collectors;

/**
 * @author francis
 * @Title: LtrimRedisSinkProcessFunction
 * @Description:
 * @Date 2022-05-31 15:28
 * @since
 */
public class LtrimRedisSinkProcessFunction implements RedisSinkProcessFunction<RowData, RedisCommandsClient> {

    private final List<LogicalType> logicalTypes;

    private final static long ttl = 604800;

    private final int length;

    public LtrimRedisSinkProcessFunction(ResolvedSchema resolvedSchema, Integer length) {
        this.logicalTypes = resolvedSchema.getColumnDataTypes().stream().map(DataType::getLogicalType).collect(Collectors.toList());
        if (logicalTypes.size() <= 1) {
            throw new IllegalArgumentException("Table column cannot be less than 2");
        }
        this.length = length;
    }


    @Override
    public void process(RowData rowData, RedisCommandsClient client, RuntimeContext var3) throws Exception {
        RowDataToDataFormatter.DataFormatString dataFormatString = RowDataToDataFormatter.createDataFormatString(logicalTypes.get(0));
        String key = dataFormatString.format(rowData, 0, logicalTypes.get(0));

        dataFormatString = RowDataToDataFormatter.createDataFormatString(logicalTypes.get(1));
        String value = dataFormatString.format(rowData, 1, logicalTypes.get(1));
        client.ltrim(ttl, key, value, length);
    }
}
