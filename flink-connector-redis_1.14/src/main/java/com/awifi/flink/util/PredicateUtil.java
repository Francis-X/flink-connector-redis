package com.awifi.flink.util;

import java.util.function.Predicate;

/**
 * @author francis
 * @Title: PredicateUtil
 * @Description:
 * @Date 2022-06-01 15:53
 * @since
 */
public class PredicateUtil {

    private PredicateUtil() {
    }

    public static <T> T checkArgument(Predicate<T> p, T t, String errorMessage) {
        if (!p.test(t)) {
            throw new IllegalArgumentException(String.valueOf(errorMessage));
        }
        return t;
    }
}
