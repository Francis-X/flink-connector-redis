package com.awifi.flink.connectors.redis.sink;

import com.awifi.flink.connectors.redis.client.RedisCommandsExecutor;
import com.awifi.flink.util.PredicateUtil;
import org.apache.flink.api.common.functions.RuntimeContext;
import org.apache.flink.table.catalog.ResolvedSchema;
import org.apache.flink.table.data.RowData;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.stream.Collectors;


/**
 * @author francis
 * @Title: RowRedisSinkProcessFunction
 * @Description:
 * @Date 2022-05-09 14:48
 * @since
 */
public class RowRedisSinkProcessFunction implements RedisSinkProcessFunction<RowData, RedisCommandsExecutor.RedisCommands> {

    private static final Logger LOG = LoggerFactory.getLogger(RowRedisSinkProcessFunction.class);

    private final List<SinkColumn> columns;

    private final long ttl;

    public RowRedisSinkProcessFunction(ResolvedSchema resolvedSchema, long ttl) {
        this.ttl = PredicateUtil.checkArgument(t -> t > 0, ttl, String.format("TTL must be greater than 0, but %s is not", ttl));
        if (resolvedSchema.getColumns().size() <= 1) {
            throw new IllegalArgumentException("Table column cannot be less than 2");
        }
        this.columns = resolvedSchema.getColumns().stream().
                map(column -> new SinkColumn(column.getName(), column.getDataType())).collect(Collectors.toList());
    }


    @Override
    public void process(RowData rowData, RedisCommandsExecutor.RedisCommands redisCommands, RuntimeContext var3) throws Exception {
        int length = rowData.getArity();

        SinkColumn keyColumn = columns.get(0);
        String key = String.valueOf(keyColumn.getRedisSerialize().format(rowData, 0));

        Object[] args = new Object[length - 1];
        for (int i = 1; i < length; i++) {
            args[i - 1] = columns.get(i).getRedisSerialize().format(rowData, i);
        }
        redisCommands.execute(ttl, key, args);
    }


    /*@Deprecated
    private String decode(RowData rowData, DataType dataType, int pos) {
        final LogicalType logicalType = dataType.getLogicalType();
        final LogicalTypeRoot typeRoot = logicalType.getTypeRoot();
        switch (typeRoot) {
            case NULL:
                return null;
            case BOOLEAN:
                return String.valueOf(rowData.getBoolean(pos));
            case TINYINT:
                return String.valueOf(rowData.getByte(pos));
            case SMALLINT:
                return String.valueOf(rowData.getShort(pos));
            case INTEGER:
            case INTERVAL_YEAR_MONTH:
                return String.valueOf(rowData.getInt(pos));
            case BIGINT:
            case INTERVAL_DAY_TIME:
                return String.valueOf(rowData.getLong(pos));
            case FLOAT:
                return String.valueOf(rowData.getFloat(pos));
            case DOUBLE:
                return String.valueOf(rowData.getDouble(pos));
            case CHAR:
            case VARCHAR:
                return rowData.getString(pos).toString();
            case BINARY:
            case VARBINARY:
                return new String(rowData.getBinary(pos));
            case DATE:
                return ISO_LOCAL_DATE.format(LocalDate.ofEpochDay(rowData.getInt(pos)));
            case TIME_WITHOUT_TIME_ZONE:
                return ISO_LOCAL_TIME.format(LocalTime.ofNanoOfDay(rowData.getInt(pos) * 1000_000L));
            case TIMESTAMP_WITHOUT_TIME_ZONE:
                final int timestampPrecision = ((TimestampType) logicalType).getPrecision();
                return SQL_TIMESTAMP_FORMAT.format((rowData.getTimestamp(pos, timestampPrecision).toLocalDateTime()));
            case TIMESTAMP_WITH_LOCAL_TIME_ZONE:
                final int zonedTimestampPrecision =
                        ((LocalZonedTimestampType) logicalType).getPrecision();
                return SQL_TIMESTAMP_WITH_LOCAL_TIMEZONE_FORMAT.format((rowData.getTimestamp(pos, zonedTimestampPrecision).toLocalDateTime()));
            case DECIMAL:
                final int precision = ((DecimalType) logicalType).getPrecision();
                final int scale = ((DecimalType) logicalType).getScale();
                DecimalData decimal = rowData.getDecimal(pos, precision, scale);
                return String.valueOf(decimal.toBigDecimal());
            case ARRAY:
            case ROW:
            case MAP:
            case MULTISET:
            case RAW:
            default:
                throw new UnsupportedOperationException("Unsupported type: " + logicalType);
        }
    }*/

}
