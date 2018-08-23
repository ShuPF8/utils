package com.spf.utils;

import org.junit.Test;

/**
 * @author ShuPF
 * @类说明：
 * @date 2018-08-23 14:23
 */
public class REXUtil {

    private static final String PHONE_REX ="(\\+86)?\\s*([0-9]{11})|[0-9]{11}";

    @Test
    public void test() {
        System.out.println("+86 18225159520".matches(PHONE_REX));
    }

}
