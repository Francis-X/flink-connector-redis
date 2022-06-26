package com.awifi.flink.connectors.redis.sink;

import com.awifi.flink.connectors.redis.client.RedisCommandsClient;
import com.awifi.flink.connectors.redis.exception.UnsupportedRedisCommandException;
import com.awifi.flink.connectors.redis.format.RowDataToDataFormatter;
import org.apache.flink.api.common.functions.RuntimeContext;
import org.apache.flink.table.catalog.ResolvedSchema;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.types.DataType;
import org.apache.flink.table.types.logical.*;
import org.apache.flink.types.RowKind;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.lang.reflect.Method;
import java.util.*;
import java.util.stream.Collectors;


/**
 * @author francis
 * @Title: RowRedisSinkProcessFunction
 * @Description:
 * @Date 2022-05-09 14:48
 * @since
 */
public class RowRedisSinkProcessFunction implements RedisSinkProcessFunction<RowData, RedisCommandsClient> {

    private static final Logger LOG = LoggerFactory.getLogger(RowRedisSinkProcessFunction.class);

    private String command;

    private final List<LogicalType> logicalTypes;

    private static final Map<String, Method> methodMap;

    private final static long ttl = 604800;

    static {
        methodMap = Arrays.stream(RedisCommandsClient.class.getDeclaredMethods()).
                collect(Collectors.toConcurrentMap(method -> method.getName().toUpperCase(), method -> method));
    }

    public RowRedisSinkProcessFunction(String command, ResolvedSchema resolvedSchema) {

        this.command = command;
        this.logicalTypes = resolvedSchema.getColumnDataTypes().stream().map(DataType::getLogicalType).collect(Collectors.toList());
        if (logicalTypes.size() <= 1) {
            throw new IllegalArgumentException("Table column cannot be less than 2");
        }
    }


    @Override
    public void process(RowData rowData, RedisCommandsClient client, RuntimeContext var3) throws Exception {
        int length = rowData.getArity();

        RowKind rowKind = rowData.getRowKind();

        RowDataToDataFormatter.DataFormatString dataFormatString = RowDataToDataFormatter.createDataFormatString(logicalTypes.get(0));
        String key = dataFormatString.format(rowData, 0, logicalTypes.get(0));

        Method cmd = methodMap.get(command);
        if (cmd == null) {
            if (LOG.isErrorEnabled()) {
                String msg = "command [" + command + "] is not support";
                LOG.error(msg);
                throw new UnsupportedRedisCommandException(msg);
            }
        }
        cmd.setAccessible(true);

        String[] args = new String[length - 1];
        for (int i = 1; i < length; i++) {
            args[i - 1] = RowDataToDataFormatter.createDataFormatString(logicalTypes.get(i)).format(rowData, i, logicalTypes.get(i));
        }

        if (args.length == 1) {
            cmd.invoke(client, ttl, key, args[0]);
            return;
        }
        cmd.invoke(client, ttl, key, args);
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
