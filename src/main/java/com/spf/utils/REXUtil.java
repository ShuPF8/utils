package com.spf.utils;

import org.junit.Test;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;

/**
 * @author ShuPF
 * @类说明：
 * @date 2018-08-23 14:23
 */
public class REXUtil {

    private static final String PHONE_REX ="(\\+86)?\\s*([0-9]{11})|[0-9]{11}";

    @Test
    public void test() throws ParseException {
        //System.out.println("+86 18225159520".matches(PHONE_REX));
        System.out.println(1001/1000+1);
        Date now = new Date();
        String str = "20180823";
        SimpleDateFormat sdf = new SimpleDateFormat("yyyyMMdd");
        Date date = sdf.parse(str);
        System.out.println(date.compareTo(now));
    }

}
