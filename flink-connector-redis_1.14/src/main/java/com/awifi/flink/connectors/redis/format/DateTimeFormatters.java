package com.awifi.flink.connectors.redis.format;

import java.time.format.DateTimeFormatter;
import java.time.format.DateTimeFormatterBuilder;
import java.time.temporal.ChronoField;

/**
 * @author francis
 * @Title: DateTimeFormatters
 * @Description:
 * @Date 2022-05-11 09:09
 * @since
 */
public class DateTimeFormatters {
    /**
     * RFC3339_TIME_FORMAT                  02:32:03.045Z
     * RFC3339_TIMESTAMP_FORMAT      2022-05-11T02:32:03.065Z
     * ISO8601_TIMESTAMP_FORMAT       2022-05-11T02:32:03.066
     * ISO8601_TIMESTAMP_WITH_LOCAL_TIMEZONE_FORMAT          2022-05-11T02:32:03.069Z
     * SQL_TIME_FORMAT                         02:32:03.069
     * SQL_TIMESTAMP_FORMAT              2022-05-11 02:32:03.069
     * SQL_TIMESTAMP_WITH_LOCAL_TIMEZONE_FORMAT         2022-05-11 02:32:03.07Z
     */
    public static final DateTimeFormatter RFC3339_TIME_FORMAT;
    public static final DateTimeFormatter RFC3339_TIMESTAMP_FORMAT;
    public static final DateTimeFormatter ISO8601_TIMESTAMP_FORMAT;
    public static final DateTimeFormatter ISO8601_TIMESTAMP_WITH_LOCAL_TIMEZONE_FORMAT;
    public static final DateTimeFormatter SQL_TIME_FORMAT;
    public static final DateTimeFormatter SQL_TIMESTAMP_FORMAT;
    public static final DateTimeFormatter SQL_TIMESTAMP_WITH_LOCAL_TIMEZONE_FORMAT;

    private DateTimeFormatters() {
    }

    static {
        RFC3339_TIME_FORMAT = (new DateTimeFormatterBuilder()).appendPattern("HH:mm:ss").appendFraction(ChronoField.NANO_OF_SECOND, 0, 9, true).appendPattern("'Z'").toFormatter();
        RFC3339_TIMESTAMP_FORMAT = (new DateTimeFormatterBuilder()).append(DateTimeFormatter.ISO_LOCAL_DATE).appendLiteral('T').append(RFC3339_TIME_FORMAT).toFormatter();
        ISO8601_TIMESTAMP_FORMAT = DateTimeFormatter.ISO_LOCAL_DATE_TIME;
        ISO8601_TIMESTAMP_WITH_LOCAL_TIMEZONE_FORMAT = (new DateTimeFormatterBuilder()).append(DateTimeFormatter.ISO_LOCAL_DATE).appendLiteral('T').append(DateTimeFormatter.ISO_LOCAL_TIME).appendPattern("'Z'").toFormatter();
        SQL_TIME_FORMAT = (new DateTimeFormatterBuilder()).appendPattern("HH:mm:ss").appendFraction(ChronoField.NANO_OF_SECOND, 0, 9, true).toFormatter();
        SQL_TIMESTAMP_FORMAT = (new DateTimeFormatterBuilder()).append(DateTimeFormatter.ISO_LOCAL_DATE).appendLiteral(' ').append(SQL_TIME_FORMAT).toFormatter();
        SQL_TIMESTAMP_WITH_LOCAL_TIMEZONE_FORMAT = (new DateTimeFormatterBuilder()).append(DateTimeFormatter.ISO_LOCAL_DATE).appendLiteral(' ').append(SQL_TIME_FORMAT).appendPattern("'Z'").toFormatter();
    }
}
