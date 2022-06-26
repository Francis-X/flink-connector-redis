package com.awifi.flink.connectors.redis.format;

import org.apache.flink.table.data.DecimalData;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.types.logical.*;

import java.io.Serializable;
import java.time.LocalDate;
import java.time.LocalTime;

import static com.awifi.flink.connectors.redis.format.DateTimeFormatters.SQL_TIMESTAMP_FORMAT;
import static com.awifi.flink.connectors.redis.format.DateTimeFormatters.SQL_TIMESTAMP_WITH_LOCAL_TIMEZONE_FORMAT;
import static java.time.format.DateTimeFormatter.ISO_LOCAL_DATE;
import static java.time.format.DateTimeFormatter.ISO_LOCAL_TIME;

/**
 * @author francis
 * @Title: RowDataTo
 * @Description:
 * @Date 2022-05-29 00:12
 * @since
 */
public class RowDataToDataFormatter implements Serializable {

    private static final long serialVersionUID = 1L;

    public interface DataFormatter<T, R> {
        R format(RowData rowData, int pos, T t);
    }


    public interface DataFormatString extends DataFormatter<LogicalType, String> {
        @Override
        String format(RowData rowData, int pos, LogicalType logicalType);
    }


    public static DataFormatString createDataFormatString(LogicalType logicalType) {
        LogicalTypeRoot typeRoot = logicalType.getTypeRoot();
        switch (typeRoot) {
            case NULL:
                return (rowData, pos, t) -> null;
            case BOOLEAN:
                return (rowData, pos, t) -> String.valueOf(rowData.getBoolean(pos));
            case TINYINT:
                return (rowData, pos, t) -> String.valueOf(rowData.getByte(pos));
            case SMALLINT:
                return (rowData, pos, t) -> String.valueOf(rowData.getShort(pos));
            case INTEGER:
            case INTERVAL_YEAR_MONTH:
                return (rowData, pos, t) -> String.valueOf(rowData.getInt(pos));
            case BIGINT:
            case INTERVAL_DAY_TIME:
                return (rowData, pos, t) -> String.valueOf(rowData.getLong(pos));
            case FLOAT:
                return (rowData, pos, t) -> String.valueOf(rowData.getFloat(pos));
            case DOUBLE:
                return (rowData, pos, t) -> String.valueOf(rowData.getDouble(pos));
            case CHAR:
            case VARCHAR:
                return (rowData, pos, t) -> rowData.getString(pos).toString();
            case BINARY:
            case VARBINARY:
                return (rowData, pos, t) -> new String(rowData.getBinary(pos));
            case DATE:
                return (rowData, pos, t) -> ISO_LOCAL_DATE.format(LocalDate.ofEpochDay(rowData.getInt(pos)));
            case TIME_WITHOUT_TIME_ZONE:
                return (rowData, pos, t) -> ISO_LOCAL_TIME.format(LocalTime.ofNanoOfDay(rowData.getInt(pos) * 1000_000L));
            case TIMESTAMP_WITHOUT_TIME_ZONE:
                return (rowData, pos, t) -> {
                    int timestampPrecision = ((TimestampType) t).getPrecision();
                    return SQL_TIMESTAMP_FORMAT.format((rowData.getTimestamp(pos, timestampPrecision).toLocalDateTime()));
                };
            case TIMESTAMP_WITH_LOCAL_TIME_ZONE:
                return (rowData, pos, t) -> {
                    final int zonedTimestampPrecision =
                            ((LocalZonedTimestampType) t).getPrecision();
                    return SQL_TIMESTAMP_WITH_LOCAL_TIMEZONE_FORMAT.format((rowData.getTimestamp(pos, zonedTimestampPrecision).toLocalDateTime()));
                };
            case DECIMAL:
                return (rowData, pos, t) -> {
                    final int precision = ((DecimalType) t).getPrecision();
                    final int scale = ((DecimalType) t).getScale();
                    DecimalData decimal = rowData.getDecimal(pos, precision, scale);
                    return String.valueOf(decimal.toBigDecimal());
                };
            case ARRAY:
            case ROW:
            case MAP:
            case MULTISET:
            case RAW:
            default:
                return (rowData, pos, t) -> {
                    throw new UnsupportedOperationException("Unsupported type: " + t);
                };
        }
    }
}
