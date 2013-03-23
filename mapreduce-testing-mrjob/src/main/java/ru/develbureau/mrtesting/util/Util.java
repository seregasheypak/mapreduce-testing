package ru.develbureau.mrtesting.util;

import java.util.Calendar;

/**
 * User: sergey.sheypak
 * Date: 23.03.13
 * Time: 16:32
 */
public final class Util {

    private Util(){}

    public static long trimMinutes(long timestamp){
        Calendar calendar = Calendar.getInstance();
        calendar.setTimeInMillis(timestamp);
        calendar.set(Calendar.MILLISECOND, 0);
        calendar.set(Calendar.SECOND, 0);
        calendar.set(Calendar.MINUTE, 0);

        return calendar.getTimeInMillis();
    }
}
