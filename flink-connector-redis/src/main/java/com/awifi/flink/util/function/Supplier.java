package com.awifi.flink.util.function;

import java.io.Serializable;

/**
 * The interface Supplier.
 *
 * @param <R> the type parameter
 * @author francis
 * @Title: Supplier
 * @Description:
 * @Date 2022 -07-05 09:55
 * @since
 */
@FunctionalInterface
public interface Supplier<R> extends Serializable {

    /**
     * Get a result
     *
     * @return the result
     */
    R get();
}
