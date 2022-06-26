package com.awifi.flink.connectors.redis.format;

import com.alibaba.fastjson.JSONObject;
import org.apache.flink.table.data.*;
import org.apache.flink.table.types.logical.*;

import javax.annotation.Nullable;
import java.io.Serializable;
import java.math.BigDecimal;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.time.ZoneOffset;
import java.time.temporal.TemporalAccessor;
import java.time.temporal.TemporalQueries;
import java.util.HashMap;
import java.util.Map;

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
public class RedisRowDataConverter implements Serializable {

    private static final long serialVersionUID = 1L;

    @FunctionalInterface
    public interface RedisSerialize<R> extends Serializable {
        R format(RowData rowData, int pos);
    }

    public static RedisSerialize createRedisSerialize(LogicalType logicalType) {
        LogicalTypeRoot typeRoot = logicalType.getTypeRoot();
        switch (typeRoot) {
            case NULL:
                return (rowData, pos) -> null;
            case BOOLEAN:
                return (rowData, pos) -> String.valueOf(rowData.getBoolean(pos));
            case TINYINT:
                return (rowData, pos) -> String.valueOf(rowData.getByte(pos));
            case SMALLINT:
                return (rowData, pos) -> String.valueOf(rowData.getShort(pos));
            case INTEGER:
            case INTERVAL_YEAR_MONTH:
                return (rowData, pos) -> String.valueOf(rowData.getInt(pos));
            case BIGINT:
            case INTERVAL_DAY_TIME:
                return (rowData, pos) -> String.valueOf(rowData.getLong(pos));
            case FLOAT:
                return (rowData, pos) -> String.valueOf(rowData.getFloat(pos));
            case DOUBLE:
                return (rowData, pos) -> String.valueOf(rowData.getDouble(pos));
            case CHAR:
            case VARCHAR:
                return (rowData, pos) -> rowData.getString(pos).toString();
            case BINARY:
            case VARBINARY:
                return (rowData, pos) -> new String(rowData.getBinary(pos));
            case DATE:
                return (rowData, pos) -> ISO_LOCAL_DATE.format(LocalDate.ofEpochDay(rowData.getInt(pos)));
            case TIME_WITHOUT_TIME_ZONE:
                return (rowData, pos) -> ISO_LOCAL_TIME.format(LocalTime.ofNanoOfDay(rowData.getInt(pos) * 1000_000L));
            case TIMESTAMP_WITHOUT_TIME_ZONE:
                return (rowData, pos) -> {
                    int timestampPrecision = ((TimestampType) logicalType).getPrecision();
                    return SQL_TIMESTAMP_FORMAT.format((rowData.getTimestamp(pos, timestampPrecision).toLocalDateTime()));
                };
            case TIMESTAMP_WITH_LOCAL_TIME_ZONE:
                return (rowData, pos) -> {
                    final int zonedTimestampPrecision =
                            ((LocalZonedTimestampType) logicalType).getPrecision();
                    return SQL_TIMESTAMP_WITH_LOCAL_TIMEZONE_FORMAT.format((rowData.getTimestamp(pos, zonedTimestampPrecision).toLocalDateTime()));
                };
            case DECIMAL:
                return (rowData, pos) -> {
                    final int precision = ((DecimalType) logicalType).getPrecision();
                    final int scale = ((DecimalType) logicalType).getScale();
                    DecimalData decimal = rowData.getDecimal(pos, precision, scale);
                    return String.valueOf(decimal.toBigDecimal());
                };
            case MAP:
                return (rowData, pos) -> {
                    Map<Object, Object> hashMap = getMapData((MapType) logicalType, rowData.getMap(pos));
                    return JSONObject.toJSONString(hashMap);
                };
            case ARRAY:
            case ROW:
            case MULTISET:
            case RAW:
            default:
                return (rowData, pos) -> {
                    throw new UnsupportedOperationException("Unsupported type: " + logicalType);
                };
        }
    }

    private static Map getMapData(MapType logicalType, MapData mapData) {
        MapType mapType = logicalType;
        LogicalType keyType = mapType.getKeyType();
        if (!keyType.is(LogicalTypeFamily.CHARACTER_STRING)) {
            throw new UnsupportedOperationException("JSON format doesn't support non-string as key type of map. The type is: " + mapType.asSummaryString());
        }
        LogicalType valueType = mapType.getValueType();
        ElementGetter elementGetter = createElementGetter(valueType);

        Map<Object, Object> hashMap = new HashMap<>();
        ArrayData keyArray = mapData.keyArray();
        ArrayData valueArray = mapData.valueArray();
        int size = mapData.size();
        for (int i = 0; i < size; i++) {
            String fieldName;
            if (keyArray.isNullAt(i)) {
                throw new UnsupportedOperationException("Map DataType key does not support null values");
            } else {
                fieldName = keyArray.getString(i).toString();
            }
            hashMap.put(fieldName, elementGetter.getElementOrNull(valueArray, i));
        }
        return hashMap;
    }


    public interface ElementGetter extends Serializable {
        @Nullable
        Object getElementOrNull(ArrayData var1, int var2);
    }

    public static ElementGetter createElementGetter(LogicalType elementType) {
        ElementGetter elementGetter;
        switch (elementType.getTypeRoot()) {
            case BOOLEAN:
                elementGetter = ArrayData::getBoolean;
                break;
            case TINYINT:
                elementGetter = ArrayData::getByte;
                break;
            case SMALLINT:
                elementGetter = ArrayData::getShort;
                break;
            case INTEGER:
            case INTERVAL_YEAR_MONTH:
                elementGetter = ArrayData::getInt;
                break;
            case BIGINT:
            case INTERVAL_DAY_TIME:
                elementGetter = ArrayData::getLong;
                break;
            case FLOAT:
                elementGetter = ArrayData::getFloat;
                break;
            case DOUBLE:
                elementGetter = ArrayData::getDouble;
                break;
            case CHAR:
            case VARCHAR:
                elementGetter = (arrayData, pos) -> arrayData.getString(pos).toString();
                break;
            case BINARY:
            case VARBINARY:
                elementGetter = ArrayData::getBinary;
                break;
            case DATE:
                elementGetter = (arrayData, pos) -> ISO_LOCAL_DATE.format(LocalDate.ofEpochDay(arrayData.getInt(pos)));
                break;
            case TIME_WITHOUT_TIME_ZONE:
                elementGetter = (arrayData, pos) -> ISO_LOCAL_TIME.format(LocalTime.ofNanoOfDay(arrayData.getInt(pos) * 1000_000L));
                break;
            case TIMESTAMP_WITHOUT_TIME_ZONE:
                elementGetter = (arrayData, pos) -> {
                    int timestampPrecision = ((TimestampType) elementType).getPrecision();
                    return SQL_TIMESTAMP_FORMAT.format((arrayData.getTimestamp(pos, timestampPrecision).toLocalDateTime()));
                };
                break;
            case TIMESTAMP_WITH_LOCAL_TIME_ZONE:
                elementGetter = (arrayData, pos) -> {
                    final int zonedTimestampPrecision =
                            ((LocalZonedTimestampType) elementType).getPrecision();
                    return SQL_TIMESTAMP_WITH_LOCAL_TIMEZONE_FORMAT.format((arrayData.getTimestamp(pos, zonedTimestampPrecision).toLocalDateTime()));
                };
                break;
            case DECIMAL:
                elementGetter = (arrayData, pos) -> {
                    final int precision = ((DecimalType) elementType).getPrecision();
                    final int scale = ((DecimalType) elementType).getScale();
                    DecimalData decimal = arrayData.getDecimal(pos, precision, scale);
                    return decimal.toBigDecimal();
                };
                break;
            case MAP:
                elementGetter = (arrayData, pos) -> getMapData((MapType) elementType, arrayData.getMap(pos));
                break;
            case ARRAY:
            case ROW:
            case MULTISET:
            case RAW:
            case NULL:
            default:
                return (arrayData, pos) -> {
                    throw new UnsupportedOperationException("Unsupported type: " + elementType);
                };
        }
        return !elementType.isNullable() ? elementGetter : (array, pos) -> array.isNullAt(pos) ? null : elementGetter.getElementOrNull(array, pos);
    }

    @FunctionalInterface
    public interface RedisDeserialize extends Serializable {
        Object deserialize(Object value);
    }

    public static RedisDeserialize createRedisDeserialize(LogicalType logicalType) {
        LogicalTypeRoot typeRoot = logicalType.getTypeRoot();
        switch (typeRoot) {
            case NULL:
                return var -> null;
            case BOOLEAN:
                return var -> Boolean.valueOf(var.toString());
            case TINYINT:
                return var -> Byte.valueOf(var.toString());
            case SMALLINT:
                return var -> Short.valueOf(var.toString());
            case INTEGER:
            case INTERVAL_YEAR_MONTH:
                return var -> Integer.valueOf(var.toString());
            case BIGINT:
            case INTERVAL_DAY_TIME:
                return var -> Long.valueOf(var.toString());
            case FLOAT:
                return var -> Float.valueOf(var.toString());
            case DOUBLE:
                return var -> Double.valueOf(var.toString());
            case CHAR:
            case VARCHAR:
                return var -> StringData.fromString(var.toString());
            case DATE:
                return var -> {
                    LocalDate localDate = ISO_LOCAL_DATE.parse(var.toString()).query(TemporalQueries.localDate());
                    return localDate.toEpochDay();
                };
            case TIME_WITHOUT_TIME_ZONE:
                return var -> {
                    LocalTime localTime = ISO_LOCAL_TIME.parse(var.toString()).query(TemporalQueries.localTime());
                    return localTime.toSecondOfDay() * 1000;
                };
            case TIMESTAMP_WITHOUT_TIME_ZONE:
                return var -> {
                    TemporalAccessor parsedTimestamp = SQL_TIMESTAMP_FORMAT.parse(var.toString());
                    LocalTime localTime = parsedTimestamp.query(TemporalQueries.localTime());
                    LocalDate localDate = parsedTimestamp.query(TemporalQueries.localDate());
                    return TimestampData.fromLocalDateTime(LocalDateTime.of(localDate, localTime));
                };
            case TIMESTAMP_WITH_LOCAL_TIME_ZONE:
                return var -> {
                    TemporalAccessor parsedTimestampWithLocalZone = SQL_TIMESTAMP_WITH_LOCAL_TIMEZONE_FORMAT.parse(var.toString());
                    LocalTime localTime = parsedTimestampWithLocalZone.query(TemporalQueries.localTime());
                    LocalDate localDate = parsedTimestampWithLocalZone.query(TemporalQueries.localDate());
                    return TimestampData.fromInstant(LocalDateTime.of(localDate, localTime).toInstant(ZoneOffset.UTC));
                };
            case DECIMAL:
                return var -> {
                    int precision = ((DecimalType) logicalType).getPrecision();
                    int scale = ((DecimalType) logicalType).getScale();
                    BigDecimal bigDecimal = new BigDecimal(var.toString());
                    return DecimalData.fromBigDecimal(bigDecimal, precision, scale);
                };
            case MAP:
            case BINARY:
            case VARBINARY:
            case ARRAY:
            case ROW:
            case MULTISET:
            case RAW:
            default:
                return var -> {
                    throw new UnsupportedOperationException("Unsupported type: " + logicalType);
                };
        }
    }

}
