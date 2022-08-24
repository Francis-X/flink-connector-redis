package com.awifi.flink.table;

import org.apache.flink.table.types.DataType;

import java.io.Serializable;

/**
 * @author francis
 * @Title: Column
 * @Description:
 * @Date 2022-06-16 14:50
 * @since
 */
public abstract class Column implements Serializable {

    private static final long serialVersionUID = 1L;

    protected final String name;
    protected final DataType dataType;

    public Column(String name, DataType dataType) {
        this.name = name;
        this.dataType = dataType;

    }

    public String getName() {
        return name;
    }

    public DataType getDataType() {
        return dataType;
    }


}
