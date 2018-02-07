package com.carpo.spark.utils;

import java.text.DateFormat;
import java.text.MessageFormat;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.*;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Author 李岩飞
 * Email eliyanfei@126.com
 * 2018/2/6
 */
public final class DateUtils {
    public static final long SECONDS = 1000;
    public static final long MINUTE = SECONDS * 60;
    public static final long HOUR = MINUTE * 60;
    public static final long DAY = HOUR * 24;
    public static final long WEEK = DAY * 7;
    public static final String DEFAULT_DATE_PATTERN = "yyyy-MM-dd";
    public static final String DEFAULT_DATE_HH_PATTERN = "yyyy-MM-dd HH";
    public static final String DEFAULT_DATE_HHMM_PATTERN = "yyyy-MM-dd HH mm";
    public static final String DEFAULT_DATE_HH_MM_PATTERN = "yyyy-MM-dd HH:mm";
    public static final String DEFAULT_DATETONUMBER_PATTERN = "yyyyMMdd";
    public static final String DEFAULT_DATEHHTONUMBER_PATTERN = "yyyyMMddHH";
    public static final String DEFAULT_DATETIME_PATTERN = "yyyy-MM-dd HH:mm:ss";
    public static final String DEFAULT_DATETIMEMILL_PATTERN = "yyyy-MM-dd HH:mm:ss.SSS";

    // 判断日期是否正确
    public static boolean isExistDate(final int yyyy, final int mm, final int dd) {
        final Calendar cal = Calendar.getInstance();
        cal.set(yyyy, mm - 1, dd);
        return cal.get(Calendar.DATE) == dd && cal.get(Calendar.MONTH) + 1 == mm && cal.get(Calendar.YEAR) == yyyy;
    }

    public static Date toDate(final int yyyy, final int mm, final int dd) {
        final Calendar cal = Calendar.getInstance();
        cal.set(yyyy, mm - 1, dd);
        return cal.getTime();
    }

    public static final String nowDate() {
        return formatDate(new Date());
    }

    public static final String nowDate(final String format) {
        return formatDate(new Date(), format);
    }

    public static final String nowDateTime() {
        return formatDateTime(new Date());
    }

    public static String formatDate(String value, String format1, String format2) {
        return formatDate(toDate(value, format1), format2);
    }

    public static String formatDate(final Date value) {
        return formatDate(value, DEFAULT_DATE_PATTERN);
    }

    public static String formatDateTime(final Date value) {
        return formatDate(value, DEFAULT_DATETIME_PATTERN);
    }

    /**
     * 以yyyy-MM-dd HH:mm:ss.SSS格式时间
     *
     * @param value
     * @return
     */
    public static final String formatDateTimeMill(final Date value) {
        return formatDate(value, DEFAULT_DATETIMEMILL_PATTERN);
    }

    public static final Date toDateTimeMill(final String value) {
        return toDate(value, DEFAULT_DATETIMEMILL_PATTERN);
    }

    public static synchronized String formatDate(final Date value, final String pattern) {
        return value == null ? "" : getSdf(pattern).format(value);
    }

    public static synchronized String formatDate(final Date value, int hour, final String pattern) {
        final Calendar cal = Calendar.getInstance();
        cal.setTime(value);
        cal.add(Calendar.HOUR_OF_DAY, hour);
        return value == null ? "" : getSdf(pattern).format(cal.getTime());
    }

    public static Date toDate(final String value) {
        return toDate(value, DEFAULT_DATETIME_PATTERN);
    }

    /**
     * 将给定的日期转换成yyyyMMdd格式的数字
     *
     * @param value
     * @return 如果传入的日期对象为空，将可能抛出格式化错误
     */
    public static int toNumber(final Date value) {
        return Integer.parseInt(formatDate(value, DEFAULT_DATETONUMBER_PATTERN));
    }

    public static int toNumber(final Date value, final String formate) {
        return Integer.parseInt(formatDate(value, formate));
    }

    public static int toNumber(final String value, final String pattern1, final String pattern2) {
        return Integer.parseInt(formatDate(toDate(value, pattern1), pattern2));
    }

    public static int toNumber(final String value) {
        return toNumber(toDate(value, DEFAULT_DATE_PATTERN));
    }

    public static String getDateFormats(final String date) {
        final Pattern pat = Pattern.compile("[0-9]{1,100}");
        final Matcher mat = pat.matcher(date);
        if (mat.matches()) {
            return "";
        }
        date.trim().replaceAll("  +", " ");
        String dat = "";
        try {
            final String[] dates = date.split(" ");
            String month = dates[0];
            if ("Jul".equals(month)) {
                month = "07";
            }
            final String[] times = dates[dates.length - 1].split(":");
            if (times[0].length() == 1) {
                times[0] = "0" + times[0];
            }
            dat = dates[2] + month + dates[1] + times[0] + times[1] + times[2];
        } catch (final Exception e) {
            e.printStackTrace();
        }
        return dat;
    }

    private static final Map<String, SimpleDateFormat> SDFS = new HashMap<String, SimpleDateFormat>(4);
    private static final Lock lockSdf = new ReentrantReadWriteLock().writeLock();

    protected static final SimpleDateFormat getSdf(final String pattern) {
        SimpleDateFormat sdf = SDFS.get(pattern);
        if (null != sdf) return sdf;
        lockSdf.lock();
        try {
            sdf = SDFS.get(pattern);
            if (null == sdf) SDFS.put(pattern, sdf = new SimpleDateFormat(pattern));
        } finally {
            lockSdf.unlock();
        }
        return sdf;
    }

    public static synchronized Date toDate(final String value, final String pattern) {
        try {
            return null == value ? null : getSdf(pattern).parse(value);
        } catch (final ParseException e) {
            return null;
        }
    }

    public static int toDateNum(final Date date, final String pattern) {
        return Integer.valueOf(formatDate(date, pattern));
    }

    public static String formatDateMDH(int time) {
        return DateUtils.formatDate(toDate(String.valueOf(time), "yyyyMMddHH"), "MM-dd HH");
    }

    public static String formatDateMD(int time) {
        return DateUtils.formatDate(toDate(String.valueOf(time), "yyyyMMdd"), "MM-dd");
    }

    public static final String millisecondToDate(final long millis) {
        return millisecondToDate(millis, "");
    }

    public static final String millisecondToDate(final long millis, final String defDate) {
        return millisecondToDate(millis, defDate, DEFAULT_DATETIME_PATTERN);
    }

    public static final String millisecondToDate(final long millis, final String defDate, final String format) {
        try {
            return formatDate(new Date(millis), format);
        } catch (final Exception e) {
            return defDate;
        }
    }

    /**
     * 0时0分0秒
     */
    public static final String CN_FMT_SEC = "{0,number,integer}分{1,number,integer}秒";
    public static final String CN_FMT = "{0,number,integer}时{1,number,integer}分{2,number,integer}秒";
    public static final String CN_FMT_MILLS = "{0,number,integer}时{1,number,integer}分{2,number,integer}秒{3,number,integer}";
    /**
     * 0HH0MM0SS
     */
    public static final String EN_FMT = "{0,number,integer}HH{1,number,integer}MM{2,number,integer}SS";
    public static final String EN_FMT_MILLS = "{0,number,integer}HH{1,number,integer}MM{2,number,integer}SS{3,number,integer}";

    public static final String onLineTimeInfo(final long baseTime) {
        return onLineTimeInfo(baseTime, CN_FMT_MILLS);
    }

    public static final String onLineTimeInfo(final long baseTime, final long nowTime) {
        return onLineTimeInfo(baseTime, nowTime, CN_FMT_MILLS);
    }

    public static final String onLineTimeInfo(final long baseTime, final String showFmt) {
        return onLineTimeInfo(baseTime, System.currentTimeMillis(), showFmt);
    }

    public static final String onLineTimeInfo(final long baseTime, final long nowTime, final String showFmt) {
        final long olTime = nowTime - baseTime;
        final long secs = olTime / 1000;
        final long h = secs / 3600;
        long ma;
        if (h == 0) {
            ma = secs;
        } else
            ma = secs % 3600;
        final long m = ma / 60;
        long sa;
        if (m == 0) sa = ma;
        else
            sa = ma % 60;
        // h+"时"+m+"分"+sa+"秒"
        return MessageFormat.format(showFmt, new Object[]{h, m, sa, olTime % 1000});
    }

    public static final String onLineTimeInfo1(final long time, final String showFmt) {
        final long secs = time / 1000;
        final long h = secs / 3600;
        long ma;
        if (h == 0) {
            ma = secs;
        } else
            ma = secs % 3600;
        final long m = ma / 60;
        long sa;
        if (m == 0) sa = ma;
        else
            sa = ma % 60;
        // h+"时"+m+"分"+sa+"秒"
        return MessageFormat.format(showFmt, new Object[]{h, m, sa, time % 1000});
    }


    private static final int[] dayArray = new int[]{31, 28, 31, 30, 31, 30, 31, 31, 30, 31, 30, 31};
    // 法定假日
    public static final String[] HOLIDAY = new String[]{"1.1", "5.1", "5.2", "5.3", "10.1", "10.2", "10.3"};
    public static final int STARTHOUR = 8;
    public static final int ENDHOUR = 5;
    private static SimpleDateFormat sdf = new SimpleDateFormat();


    /**
     * 取得中国指定日期的所处星期的第一天
     *
     * @param date 指定日期。
     * @return 指定日期的所处星期的第一天
     */
    public static java.util.Date getFirstDayOfWeekByCH(final java.util.Date date) {
        /**
         * 详细设计： 1.如果date是星期日，则减0天 2.如果date是星期一，则减1天 3.如果date是星期二，则减2天
         * 4.如果date是星期三，则减3天 5.如果date是星期四，则减4天 6.如果date是星期五，则减5天 7.如果date是星期六，则减6天
         */
        final GregorianCalendar gc = (GregorianCalendar) Calendar.getInstance();
        gc.setTime(date);
        switch (gc.get(Calendar.DAY_OF_WEEK)) {
            case Calendar.SUNDAY:
                gc.add(Calendar.DATE, -6);
                break;
            case Calendar.MONDAY:
                gc.add(Calendar.DATE, 0);
                break;
            case Calendar.TUESDAY:
                gc.add(Calendar.DATE, 1);
                break;
            case Calendar.WEDNESDAY:
                gc.add(Calendar.DATE, -2);
                break;
            case Calendar.THURSDAY:
                gc.add(Calendar.DATE, -3);
                break;
            case Calendar.FRIDAY:
                gc.add(Calendar.DATE, -4);
                break;
            case Calendar.SATURDAY:
                gc.add(Calendar.DATE, -5);
                break;
        }
        return gc.getTime();
    }

    /**
     * 得到一个卡林达标准的当前时间对象
     *
     * @return
     */
    public static Calendar getCalendar() {
        return Calendar.getInstance();
    }

    /**
     * 得到当前系统时间的一个带毫秒的字符串格式
     *
     * @return String
     */
    public static String getDateMilliFormat() {
        return getDateMilliFormat(Calendar.getInstance());
    }

    /**
     * 将一个Calendar对象格式化成一个带毫秒的字符串表现形式
     *
     * @param cal
     * @return String
     */
    public static String getDateMilliFormat(final java.util.Calendar cal) {
        return getDateFormat(cal, "yyyy-MM-dd HH:mm:ss,SSS");
    }

    /**
     * 将一个java.util.Date对象格式化成一个带毫秒的字符串表现形式
     *
     * @param date
     * @return String
     */
    public static String getDateMilliFormat(final java.util.Date date) {
        return getDateFormat(date, "yyyy-MM-dd HH:mm:ss,SSS");
    }

    /**
     * @param strDate
     * @return java.util.Calendar
     */
    public static Calendar parseCalendarMilliFormat(final String strDate) {
        return parseCalendarFormat(strDate, "yyyy-MM-dd HH:mm:ss,SSS");
    }

    /**
     * @param strDate
     * @return java.util.Date
     */
    public static Date parseDateMilliFormat(final String strDate) {
        return parseDateFormat(strDate, "yyyy-MM-dd HH:mm:ss,SSS");
    }

    /**
     * @return String
     */
    public static String getDateSecondFormat() {
        return getDateSecondFormat(Calendar.getInstance());
    }

    /**
     * @param cal
     * @return String
     */
    public static String getDateSecondFormat(final java.util.Calendar cal) {
        final String pattern = "yyyy-MM-dd HH:mm:ss";
        return getDateFormat(cal, pattern);
    }

    /**
     * @param date
     * @return String
     */
    public static String getDateSecondFormat(final java.util.Date date) {
        final String pattern = "yyyy-MM-dd HH:mm:ss";
        return getDateFormat(date, pattern);
    }

    /**
     * @param strDate
     * @return java.util.Calendar
     */
    public static Calendar parseCalendarSecondFormat(final String strDate) {
        final String pattern = "yyyy-MM-dd HH:mm:ss";
        return parseCalendarFormat(strDate, pattern);
    }

    /**
     * @param strDate
     * @return java.util.Date
     */
    public static Date parseDateSecondFormat(final String strDate) {
        final String pattern = "yyyy-MM-dd HH:mm:ss";
        return parseDateFormat(strDate, pattern);
    }

    /**
     * @return String
     */
    public static String getDateMinuteFormat() {
        final Calendar cal = Calendar.getInstance();
        return getDateMinuteFormat(cal);
    }

    /**
     * @param cal
     * @return String
     */
    public static String getDateMinuteFormat(final java.util.Calendar cal) {
        final String pattern = "yyyy-MM-dd HH:mm";
        return getDateFormat(cal, pattern);
    }

    /**
     * @param date
     * @return String
     */
    public static String getDateMinuteFormat(final java.util.Date date) {
        final String pattern = "yyyy-MM-dd HH:mm";
        return getDateFormat(date, pattern);
    }

    /**
     * @param strDate
     * @return java.util.Calendar
     */
    public static Calendar parseCalendarMinuteFormat(final String strDate) {
        final String pattern = "yyyy-MM-dd HH:mm";
        return parseCalendarFormat(strDate, pattern);
    }

    /**
     * @param strDate
     * @return java.util.Date
     */
    public static Date parseDateMinuteFormat(final String strDate) {
        final String pattern = "yyyy-MM-dd HH:mm";
        return parseDateFormat(strDate, pattern);
    }

    /**
     * @return String
     */
    public static String getDateDayFormat() {
        final Calendar cal = Calendar.getInstance();
        return getDateDayFormat(cal);
    }

    /**
     * @param cal
     * @return String
     */
    public static String getDateDayFormat(final java.util.Calendar cal) {
        final String pattern = "yyyy-MM-dd";
        return getDateFormat(cal, pattern);
    }

    /**
     * @param date
     * @return String
     */
    public static String getDateDayFormat(final java.util.Date date) {
        final String pattern = "yyyy-MM-dd";
        return getDateFormat(date, pattern);
    }

    /**
     * @param strDate
     * @return java.util.Calendar
     */
    public static Calendar parseCalendarDayFormat(final String strDate) {
        final String pattern = "yyyy-MM-dd";
        return parseCalendarFormat(strDate, pattern);
    }

    public static String parseDateStringFormat(final String strDate, final String pattern, final String pattern1) {
        final Date date = parseDateFormat(strDate, pattern);
        return getCurrentDateFormat(date, pattern1);
    }

    /**
     * @param strDate
     * @return java.util.Date
     */
    public static Date parseDateDayFormat(final String strDate) {
        final String pattern = "yyyy-MM-dd";
        return parseDateFormat(strDate, pattern);
    }

    /**
     * @return String
     */
    public static String getDateFileFormat() {
        final Calendar cal = Calendar.getInstance();
        return getDateFileFormat(cal);
    }

    /**
     * @param cal
     * @return String
     */
    public static String getDateFileFormat(final java.util.Calendar cal) {
        final String pattern = "yyyy-MM-dd_HH-mm-ss";
        return getDateFormat(cal, pattern);
    }

    /**
     * @param date
     * @return String
     */
    public static String getDateFileFormat(final java.util.Date date) {
        final String pattern = "yyyy-MM-dd_HH-mm-ss";
        return getDateFormat(date, pattern);
    }

    /**
     * @param strDate
     * @return java.util.Calendar
     */
    public static Calendar parseCalendarFileFormat(final String strDate) {
        final String pattern = "yyyy-MM-dd_HH-mm-ss";
        return parseCalendarFormat(strDate, pattern);
    }

    /**
     * @param strDate
     * @return java.util.Date
     */
    public static Date parseDateFileFormat(final String strDate) {
        final String pattern = "yyyy-MM-dd_HH-mm-ss";
        return parseDateFormat(strDate, pattern);
    }

    /**
     * @return String
     */
    public static String getDateW3CFormat() {
        final Calendar cal = Calendar.getInstance();
        return getDateW3CFormat(cal);
    }

    /**
     * @param cal
     * @return String
     */
    public static String getDateW3CFormat(final java.util.Calendar cal) {
        final String pattern = "yyyy-MM-dd HH:mm:ss";
        return getDateFormat(cal, pattern);
    }

    /**
     * @param date
     * @return String
     */
    public static String getDateW3CFormat(final java.util.Date date) {
        final String pattern = "yyyy-MM-dd HH:mm:ss";
        return getDateFormat(date, pattern);
    }

    /**
     * @param strDate
     * @return java.util.Calendar
     */
    public static Calendar parseCalendarW3CFormat(final String strDate) {
        final String pattern = "yyyy-MM-dd HH:mm:ss";
        return parseCalendarFormat(strDate, pattern);
    }

    /**
     * @param strDate
     * @return java.util.Date
     */
    public static Date parseDateW3CFormat(final String strDate) {
        final String pattern = "yyyy-MM-dd HH:mm:ss";
        return parseDateFormat(strDate, pattern);
    }

    /**
     * @param cal
     * @return String
     */
    public static String getDateFormat(final java.util.Calendar cal) {
        final String pattern = "yyyy-MM-dd HH:mm:ss";
        return getDateFormat(cal, pattern);
    }

    /**
     * @param date
     * @return String
     */
    public static String getDateFormat(final java.util.Date date) {
        final String pattern = "yyyy-MM-dd HH:mm:ss";
        return getDateFormat(date, pattern);
    }

    /**
     * @param strDate
     * @return java.util.Calendar
     */
    public static Calendar parseCalendarFormat(final String strDate) {
        final String pattern = "yyyy-MM-dd HH:mm:ss";
        return parseCalendarFormat(strDate, pattern);
    }

    /**
     * @param strDate
     * @return java.util.Date
     */
    public static Date parseDateFormat(final String strDate) {
        final String pattern = "yyyy-MM-dd HH:mm:ss";
        return parseDateFormat(strDate, pattern);
    }

    /**
     * @param cal
     * @param pattern
     * @return String
     */
    public static String getDateFormat(final java.util.Calendar cal, final String pattern) {
        return getDateFormat(cal.getTime(), pattern);
    }

    /**
     * @param date
     * @param pattern
     * @return String
     */
    public static String getDateFormat(final java.util.Date date, final String pattern) {
        synchronized (sdf) {
            if (null == date) return "";
            String str = null;
            sdf.applyPattern(pattern);
            str = sdf.format(date);
            return str;
        }
    }

    public static String getCurrentDateFormat(java.util.Date date, String pattern) {
        if (date == null) {
            date = new Date();
        }
        if (pattern == null) {
            pattern = DEFAULT_DATE_HH_PATTERN;
        }
        return getDateFormat(date, pattern);
    }

    /**
     * 用于在WEB上显示时间时可使用此方法,如果将要返回的值为空,最终将返回一个WEB上的标准空格,
     * 因此可以解决有时候时间对象为空时页面上显示null字样的问题
     *
     * @param date
     * @param pattern
     * @return
     */
    public static String getDateFormatByWEB(final Date date, final String pattern) {
        final String returnValue = getDateFormat(date, pattern);
        if (null == returnValue || "".equals("returnValue")) return "&nbsp;";
        return returnValue;
    }

    /**
     * 根据传入的时间得到一个"年月"格式的字符串,如果传入的是null,以当前时间为准.
     *
     * @param date
     * @return 一个"年月"格式的字符串,如"200801"表示为2008年1月
     */
    public static String getYearMonth(Date date) {
        if (null == date) date = new Date();
        return getDateFormat(date, "yyyyMM");
    }

    /**
     * @param strDate
     * @param pattern
     * @return java.util.Calendar
     */
    public static Calendar parseCalendarFormat(final String strDate, final String pattern) {
        synchronized (sdf) {
            Calendar cal = null;
            sdf.applyPattern(pattern);
            try {
                sdf.parse(strDate);
                cal = sdf.getCalendar();
            } catch (final Exception e) {
            }
            return cal;
        }
    }

    /**
     * @param strDate
     * @param pattern
     * @return java.util.Date
     */
    public static Date parseDateFormat(final String strDate, final String pattern) {
        synchronized (sdf) {
            Date date = null;
            sdf.applyPattern(pattern);
            try {
                date = sdf.parse(strDate);
            } catch (final Exception e) {
            }
            return date;
        }
    }

    public static int getLastDayOfMonth(final int month) {
        if (month < 1 || month > 12) {
            return -1;
        }
        int retn = 0;
        if (month == 2) {
            if (isLeapYear()) {
                retn = 29;
            } else {
                retn = dayArray[month - 1];
            }
        } else {
            retn = dayArray[month - 1];
        }
        return retn;
    }

    public static int getLastDayOfMonth(final int year, final int month) {
        if (month < 1 || month > 12) {
            return -1;
        }
        int retn = 0;
        if (month == 2) {
            if (isLeapYear(year)) {
                retn = 29;
            } else {
                retn = dayArray[month - 1];
            }
        } else {
            retn = dayArray[month - 1];
        }
        return retn;
    }

    public static boolean isLeapYear() {
        final Calendar cal = Calendar.getInstance();
        final int year = cal.get(Calendar.YEAR);
        return isLeapYear(year);
    }

    public static boolean isLeapYear(final int year) {
        /**
         * 详细设计： 1.被400整除是闰年，否则： 2.不能被4整除则不是闰年 3.能被4整除同时不能被100整除则是闰年
         * 3.能被4整除同时能被100整除则不是闰年
         */
        if (year % 400 == 0) return true;
        else if (year % 4 == 0) {
            if (year % 100 == 0) return false;
            return true;
        } else
            return false;
    }

    /**
     * 判断指定日期的年份是否是闰年
     *
     * @param date 指定日期。
     * @return 是否闰年
     */
    public static boolean isLeapYear(final java.util.Date date) {
        /**
         * 详细设计： 1.被400整除是闰年，否则： 2.不能被4整除则不是闰年 3.能被4整除同时不能被100整除则是闰年
         * 3.能被4整除同时能被100整除则不是闰年
         */
        // int year = date.getYear();
        final GregorianCalendar gc = (GregorianCalendar) Calendar.getInstance();
        gc.setTime(date);
        final int year = gc.get(Calendar.YEAR);
        return isLeapYear(year);
    }

    public static boolean isLeapYear(final java.util.Calendar gc) {
        /**
         * 详细设计： 1.被400整除是闰年，否则： 2.不能被4整除则不是闰年 3.能被4整除同时不能被100整除则是闰年
         * 3.能被4整除同时能被100整除则不是闰年
         */
        final int year = gc.get(Calendar.YEAR);
        return isLeapYear(year);
    }

    /**
     * 得到指定日期的前一个工作日
     *
     * @param date 指定日期。
     * @return 指定日期的前一个工作日
     */
    public static java.util.Date getPreviousWeekDay(final java.util.Date date) {
        {
            /**
             * 详细设计： 1.如果date是星期日，则减3天 2.如果date是星期六，则减2天 3.否则减1天
             */
            final GregorianCalendar gc = (GregorianCalendar) Calendar.getInstance();
            gc.setTime(date);
            return getPreviousWeekDay(gc);
            // switch ( gc.get( Calendar.DAY_OF_WEEK ) )
            // {
            // case ( Calendar.MONDAY ):
            // gc.add( Calendar.DATE, -3 );
            // break;
            // case ( Calendar.SUNDAY ):
            // gc.add( Calendar.DATE, -2 );
            // break;
            // default:
            // gc.add( Calendar.DATE, -1 );
            // break;
            // }
            // return gc.getTime();
        }
    }

    public static java.util.Date getPreviousWeekDay(final java.util.Calendar gc) {
        {
            /**
             * 详细设计： 1.如果date是星期日，则减3天 2.如果date是星期六，则减2天 3.否则减1天
             */
            switch (gc.get(Calendar.DAY_OF_WEEK)) {
                case Calendar.MONDAY:
                    gc.add(Calendar.DATE, -3);
                    break;
                case Calendar.SUNDAY:
                    gc.add(Calendar.DATE, -2);
                    break;
                default:
                    gc.add(Calendar.DATE, -1);
                    break;
            }
            return gc.getTime();
        }
    }

    /**
     * 得到指定日期的后一个工作日
     *
     * @param date 指定日期。
     * @return 指定日期的后一个工作日
     */
    public static java.util.Date getNextWeekDay(final java.util.Date date) {
        /**
         * 详细设计： 1.如果date是星期五，则加3天 2.如果date是星期六，则加2天 3.否则加1天
         */
        final GregorianCalendar gc = (GregorianCalendar) Calendar.getInstance();
        gc.setTime(date);
        switch (gc.get(Calendar.DAY_OF_WEEK)) {
            case Calendar.FRIDAY:
                gc.add(Calendar.DATE, 3);
                break;
            case Calendar.SATURDAY:
                gc.add(Calendar.DATE, 2);
                break;
            default:
                gc.add(Calendar.DATE, 1);
                break;
        }
        return gc.getTime();
    }

    public static java.util.Calendar getNextWeekDay(final java.util.Calendar gc) {
        /**
         * 详细设计： 1.如果date是星期五，则加3天 2.如果date是星期六，则加2天 3.否则加1天
         */
        switch (gc.get(Calendar.DAY_OF_WEEK)) {
            case Calendar.FRIDAY:
                gc.add(Calendar.DATE, 3);
                break;
            case Calendar.SATURDAY:
                gc.add(Calendar.DATE, 2);
                break;
            default:
                gc.add(Calendar.DATE, 1);
                break;
        }
        return gc;
    }

    /**
     * 取得指定日期的下一个月的最后一天
     *
     * @param date 指定日期。
     * @return 指定日期的下一个月的最后一天
     */
    public static java.util.Date getLastDayOfNextMonth(final java.util.Date date) {
        /**
         * 详细设计： 1.调用getNextMonth设置当前时间 2.以1为基础，调用getLastDayOfMonth
         */
        final GregorianCalendar gc = (GregorianCalendar) Calendar.getInstance();
        gc.setTime(date);
        gc.setTime(getNextMonth(gc.getTime()));
        gc.setTime(getLastDayOfMonth(gc.getTime()));
        return gc.getTime();
    }

    /**
     * 取得指定日期的下一个星期的最后一天
     *
     * @param date 指定日期。
     * @return 指定日期的下一个星期的最后一天
     */
    public static java.util.Date getLastDayOfNextWeek(final java.util.Date date) {
        /**
         * 详细设计： 1.调用getNextWeek设置当前时间 2.以1为基础，调用getLastDayOfWeek
         */
        final GregorianCalendar gc = (GregorianCalendar) Calendar.getInstance();
        gc.setTime(date);
        gc.setTime(getNextWeek(gc.getTime()));
        gc.setTime(getLastDayOfWeek(gc.getTime()));
        return gc.getTime();
    }

    /**
     * 取得指定日期的下一个月的第一天
     *
     * @param date 指定日期。
     * @return 指定日期的下一个月的第一天
     */
    public static java.util.Date getFirstDayOfNextMonth(final java.util.Date date) {
        /**
         * 详细设计： 1.调用getNextMonth设置当前时间 2.以1为基础，调用getFirstDayOfMonth
         */
        final GregorianCalendar gc = (GregorianCalendar) Calendar.getInstance();
        gc.setTime(date);
        gc.setTime(getNextMonth(gc.getTime()));
        gc.setTime(getFirstDayOfMonth(gc.getTime()));
        return gc.getTime();
    }

    public static java.util.Calendar getFirstDayOfNextMonth(final java.util.Calendar gc) {
        /**
         * 详细设计： 1.调用getNextMonth设置当前时间 2.以1为基础，调用getFirstDayOfMonth
         */
        gc.setTime(getNextMonth(gc.getTime()));
        gc.setTime(getFirstDayOfMonth(gc.getTime()));
        return gc;
    }

    /**
     * 取得指定日期的下一个星期的第一天
     *
     * @param date 指定日期。
     * @return 指定日期的下一个星期的第一天
     */
    public static java.util.Date getFirstDayOfNextWeek(final java.util.Date date) {
        /**
         * 详细设计： 1.调用getNextWeek设置当前时间 2.以1为基础，调用getFirstDayOfWeek
         */
        final GregorianCalendar gc = (GregorianCalendar) Calendar.getInstance();
        gc.setTime(date);
        gc.setTime(getNextWeek(gc.getTime()));
        gc.setTime(getFirstDayOfWeek(gc.getTime()));
        return gc.getTime();
    }

    public static java.util.Calendar getFirstDayOfNextWeek(final java.util.Calendar gc) {
        /**
         * 详细设计： 1.调用getNextWeek设置当前时间 2.以1为基础，调用getFirstDayOfWeek
         */
        gc.setTime(getNextWeek(gc.getTime()));
        gc.setTime(getFirstDayOfWeek(gc.getTime()));
        return gc;
    }

    /**
     * 取得指定日期的下一个月
     *
     * @param date 指定日期。
     * @return 指定日期的下一个月
     */
    public static java.util.Date getNextMonth(final java.util.Date date) {
        /**
         * 详细设计： 1.指定日期的月份加1
         */
        final GregorianCalendar gc = (GregorianCalendar) Calendar.getInstance();
        gc.setTime(date);
        gc.add(Calendar.MONTH, 1);
        return gc.getTime();
    }

    /**
     * 取得指定日期的上一个月
     *
     * @param date 指定日期。
     * @return 指定日期的上一个月
     */
    public static java.util.Date getPreviewMonth(final java.util.Date date) {
        /**
         * 详细设计： 1.指定日期的月份加1
         */
        final GregorianCalendar gc = (GregorianCalendar) Calendar.getInstance();
        gc.setTime(date);
        gc.add(Calendar.MONTH, -1);
        return gc.getTime();
    }

    public static String getPreviewMonth(final String date) {
        /**
         * 详细设计： 1.指定日期的月份加1
         */
        String dateBefore = null;
        if ("01".endsWith(date.substring(6, 7))) {
            dateBefore = Integer.parseInt(date.substring(0, 4)) - 1 + "-12";
        } else {
            final int m = Integer.parseInt(date.substring(5, 7)) - 1;
            if (m < 10) {
                dateBefore = date.substring(0, 4) + "-0" + m;
            } else {
                dateBefore = date.substring(0, 4) + "-" + m;
            }
        }
        return dateBefore;
    }

    public static java.util.Calendar getNextMonth(final java.util.Calendar gc) {
        /**
         * 详细设计： 1.指定日期的月份加1
         */
        gc.add(Calendar.MONTH, 1);
        return gc;
    }

    /**
     * 获取给定时间的上一个小时点
     *
     * @param curDate
     * @return
     */
    public static final Date getPreviousHour(final Date curDate) {
        if (null == curDate) return curDate;
        final Calendar cal = Calendar.getInstance();
        cal.setTime(curDate);
        cal.add(Calendar.HOUR_OF_DAY, -1);
        return cal.getTime();
    }

    /**
     * 获取给定时间的下一个小时点
     *
     * @param curDate
     * @return
     */
    public static final Date getNextHour(final Date curDate) {
        if (null == curDate) return curDate;
        final Calendar cal = Calendar.getInstance();
        cal.setTime(curDate);
        cal.add(Calendar.HOUR_OF_DAY, 1);
        return cal.getTime();
    }

    /**
     * 取得指定日期的下一天
     *
     * @param date 指定日期。
     * @return 指定日期的下一天
     */
    public static java.util.Date getNextDay(final java.util.Date date) {
        /**
         * 详细设计： 1.指定日期加1天
         */
        final GregorianCalendar gc = (GregorianCalendar) Calendar.getInstance();
        gc.setTime(date);
        gc.add(Calendar.DATE, 1);
        return gc.getTime();
    }

    public static java.util.Calendar getNextDay(final java.util.Calendar gc) {
        /**
         * 详细设计： 1.指定日期加1天
         */
        gc.add(Calendar.DATE, 1);
        return gc;
    }

    /**
     * 取得指定日期的下一个星期
     *
     * @param date 指定日期。
     * @return 指定日期的下一个星期
     */
    public static java.util.Date getNextWeek(final java.util.Date date) {
        /**
         * 详细设计： 1.指定日期加7天
         */
        final GregorianCalendar gc = (GregorianCalendar) Calendar.getInstance();
        gc.setTime(date);
        gc.add(Calendar.DATE, 7);
        return gc.getTime();
    }

    public static Date getPreviousWeek(final Date date) {
        final GregorianCalendar gc = (GregorianCalendar) Calendar.getInstance();
        gc.setTime(date);
        gc.add(Calendar.DATE, -7);
        return gc.getTime();
    }

    public static java.util.Calendar getNextWeek(final java.util.Calendar gc) {
        /**
         * 详细设计： 1.指定日期加7天
         */
        gc.add(Calendar.DATE, 7);
        return gc;
    }

    public static Calendar getPreviousWeek(final Calendar gc) {
        gc.add(Calendar.DATE, -7);
        return gc;
    }

    /**
     * 取得指定日期的所处星期的最后一天
     *
     * @param date 指定日期。
     * @return 指定日期的所处星期的最后一天
     */
    public static java.util.Date getLastDayOfWeek(final java.util.Date date) {
        /**
         * 详细设计： 1.如果date是星期日，则加6天 2.如果date是星期一，则加5天 3.如果date是星期二，则加4天
         * 4.如果date是星期三，则加3天 5.如果date是星期四，则加2天 6.如果date是星期五，则加1天 7.如果date是星期六，则加0天
         */
        final GregorianCalendar gc = (GregorianCalendar) Calendar.getInstance();
        gc.setTime(date);
        switch (gc.get(Calendar.DAY_OF_WEEK)) {
            case Calendar.SUNDAY:
                gc.add(Calendar.DATE, 6);
                break;
            case Calendar.MONDAY:
                gc.add(Calendar.DATE, 5);
                break;
            case Calendar.TUESDAY:
                gc.add(Calendar.DATE, 4);
                break;
            case Calendar.WEDNESDAY:
                gc.add(Calendar.DATE, 3);
                break;
            case Calendar.THURSDAY:
                gc.add(Calendar.DATE, 2);
                break;
            case Calendar.FRIDAY:
                gc.add(Calendar.DATE, 1);
                break;
            case Calendar.SATURDAY:
                gc.add(Calendar.DATE, 0);
                break;
        }
        return gc.getTime();
    }

    /**
     * 取得指定日期的所处星期的第一天
     *
     * @param date 指定日期。
     * @return 指定日期的所处星期的第一天
     */
    public static java.util.Date getFirstDayOfWeek(final java.util.Date date) {
        /**
         * 详细设计： 1.如果date是星期日，则减0天 2.如果date是星期一，则减1天 3.如果date是星期二，则减2天
         * 4.如果date是星期三，则减3天 5.如果date是星期四，则减4天 6.如果date是星期五，则减5天 7.如果date是星期六，则减6天
         */
        final GregorianCalendar gc = (GregorianCalendar) Calendar.getInstance();
        gc.setTime(date);
        switch (gc.get(Calendar.DAY_OF_WEEK)) {
            case Calendar.SUNDAY:
                gc.add(Calendar.DATE, 0);
                break;
            case Calendar.MONDAY:
                gc.add(Calendar.DATE, -1);
                break;
            case Calendar.TUESDAY:
                gc.add(Calendar.DATE, -2);
                break;
            case Calendar.WEDNESDAY:
                gc.add(Calendar.DATE, -3);
                break;
            case Calendar.THURSDAY:
                gc.add(Calendar.DATE, -4);
                break;
            case Calendar.FRIDAY:
                gc.add(Calendar.DATE, -5);
                break;
            case Calendar.SATURDAY:
                gc.add(Calendar.DATE, -6);
                break;
        }
        return gc.getTime();
    }

    public static java.util.Calendar getFirstDayOfWeek(final java.util.Calendar gc) {
        /**
         * 详细设计： 1.如果date是星期日，则减0天 2.如果date是星期一，则减1天 3.如果date是星期二，则减2天
         * 4.如果date是星期三，则减3天 5.如果date是星期四，则减4天 6.如果date是星期五，则减5天 7.如果date是星期六，则减6天
         */
        switch (gc.get(Calendar.DAY_OF_WEEK)) {
            case Calendar.SUNDAY:
                gc.add(Calendar.DATE, 0);
                break;
            case Calendar.MONDAY:
                gc.add(Calendar.DATE, -1);
                break;
            case Calendar.TUESDAY:
                gc.add(Calendar.DATE, -2);
                break;
            case Calendar.WEDNESDAY:
                gc.add(Calendar.DATE, -3);
                break;
            case Calendar.THURSDAY:
                gc.add(Calendar.DATE, -4);
                break;
            case Calendar.FRIDAY:
                gc.add(Calendar.DATE, -5);
                break;
            case Calendar.SATURDAY:
                gc.add(Calendar.DATE, -6);
                break;
        }
        return gc;
    }

    /**
     * 取得指定日期的所处月份的最后一天
     *
     * @param date 指定日期。
     * @return 指定日期的所处月份的最后一天
     */
    public static java.util.Date getLastDayOfMonth(final java.util.Date date) {
        /**
         * 详细设计： 1.如果date在1月，则为31日 2.如果date在2月，则为28日 3.如果date在3月，则为31日
         * 4.如果date在4月，则为30日 5.如果date在5月，则为31日 6.如果date在6月，则为30日 7.如果date在7月，则为31日
         * 8.如果date在8月，则为31日 9.如果date在9月，则为30日 10.如果date在10月，则为31日
         * 11.如果date在11月，则为30日 12.如果date在12月，则为31日 1.如果date在闰年的2月，则为29日
         */
        final GregorianCalendar gc = (GregorianCalendar) Calendar.getInstance();
        gc.setTime(date);
        switch (gc.get(Calendar.MONTH)) {
            case 0:
                gc.set(Calendar.DAY_OF_MONTH, 31);
                break;
            case 1:
                gc.set(Calendar.DAY_OF_MONTH, 28);
                break;
            case 2:
                gc.set(Calendar.DAY_OF_MONTH, 31);
                break;
            case 3:
                gc.set(Calendar.DAY_OF_MONTH, 30);
                break;
            case 4:
                gc.set(Calendar.DAY_OF_MONTH, 31);
                break;
            case 5:
                gc.set(Calendar.DAY_OF_MONTH, 30);
                break;
            case 6:
                gc.set(Calendar.DAY_OF_MONTH, 31);
                break;
            case 7:
                gc.set(Calendar.DAY_OF_MONTH, 31);
                break;
            case 8:
                gc.set(Calendar.DAY_OF_MONTH, 30);
                break;
            case 9:
                gc.set(Calendar.DAY_OF_MONTH, 31);
                break;
            case 10:
                gc.set(Calendar.DAY_OF_MONTH, 30);
                break;
            case 11:
                gc.set(Calendar.DAY_OF_MONTH, 31);
                break;
        }
        // 检查闰年
        if (gc.get(Calendar.MONTH) == Calendar.FEBRUARY && isLeapYear(gc.get(Calendar.YEAR))) {
            gc.set(Calendar.DAY_OF_MONTH, 29);
        }
        return gc.getTime();
    }

    public static java.util.Calendar getLastDayOfMonth(final java.util.Calendar gc) {
        /**
         * 详细设计： 1.如果date在1月，则为31日 2.如果date在2月，则为28日 3.如果date在3月，则为31日
         * 4.如果date在4月，则为30日 5.如果date在5月，则为31日 6.如果date在6月，则为30日 7.如果date在7月，则为31日
         * 8.如果date在8月，则为31日 9.如果date在9月，则为30日 10.如果date在10月，则为31日
         * 11.如果date在11月，则为30日 12.如果date在12月，则为31日 1.如果date在闰年的2月，则为29日
         */
        switch (gc.get(Calendar.MONTH)) {
            case 0:
                gc.set(Calendar.DAY_OF_MONTH, 31);
                break;
            case 1:
                gc.set(Calendar.DAY_OF_MONTH, 28);
                break;
            case 2:
                gc.set(Calendar.DAY_OF_MONTH, 31);
                break;
            case 3:
                gc.set(Calendar.DAY_OF_MONTH, 30);
                break;
            case 4:
                gc.set(Calendar.DAY_OF_MONTH, 31);
                break;
            case 5:
                gc.set(Calendar.DAY_OF_MONTH, 30);
                break;
            case 6:
                gc.set(Calendar.DAY_OF_MONTH, 31);
                break;
            case 7:
                gc.set(Calendar.DAY_OF_MONTH, 31);
                break;
            case 8:
                gc.set(Calendar.DAY_OF_MONTH, 30);
                break;
            case 9:
                gc.set(Calendar.DAY_OF_MONTH, 31);
                break;
            case 10:
                gc.set(Calendar.DAY_OF_MONTH, 30);
                break;
            case 11:
                gc.set(Calendar.DAY_OF_MONTH, 31);
                break;
        }
        // 检查闰年
        if (gc.get(Calendar.MONTH) == Calendar.FEBRUARY && isLeapYear(gc.get(Calendar.YEAR))) {
            gc.set(Calendar.DAY_OF_MONTH, 29);
        }
        return gc;
    }

    /**
     * 取得指定日期的所处月份的第一天
     *
     * @param date 指定日期。
     * @return 指定日期的所处月份的第一天
     */
    public static java.util.Date getFirstDayOfMonth(final java.util.Date date) {
        /**
         * 详细设计： 1.设置为1号
         */
        final GregorianCalendar gc = (GregorianCalendar) Calendar.getInstance();
        gc.setTime(date);
        gc.set(Calendar.DAY_OF_MONTH, 1);
        return gc.getTime();
    }

    public static java.util.Calendar getFirstDayOfMonth(final java.util.Calendar gc) {
        /**
         * 详细设计： 1.设置为1号
         */
        gc.set(Calendar.DAY_OF_MONTH, 1);
        return gc;
    }

    /**
     * 将日期对象转换成为指定ORA日期、时间格式的字符串形式。如果日期对象为空，返回 一个空字符串对象，而不是一个空对象。
     *
     * @param theDate 将要转换为字符串的日期对象。
     * @param hasTime 如果返回的字符串带时间则为true
     * @return 转换的结果
     */
    public static String toOraString(final Date theDate, final boolean hasTime) {
        /**
         * 详细设计： 1.如果有时间，则设置格式为getOraDateTimeFormat()的返回值
         * 2.否则设置格式为getOraDateFormat()的返回值 3.调用toString(Date theDate, DateFormat
         * theDateFormat)
         */
        DateFormat theFormat;
        if (hasTime) {
            theFormat = getOraDateTimeFormat();
        } else {
            theFormat = getOraDateFormat();
        }
        return toString(theDate, theFormat);
    }

    /**
     * 将日期对象转换成为指定日期、时间格式的字符串形式。如果日期对象为空，返回 一个空字符串对象，而不是一个空对象。
     *
     * @param theDate 将要转换为字符串的日期对象。
     * @param hasTime 如果返回的字符串带时间则为true
     * @return 转换的结果
     */
    public static String toString(final Date theDate, final boolean hasTime) {
        /**
         * 详细设计： 1.如果有时间，则设置格式为getDateTimeFormat的返回值 2.否则设置格式为getDateFormat的返回值
         * 3.调用toString(Date theDate, DateFormat theDateFormat)
         */
        DateFormat theFormat;
        if (hasTime) {
            theFormat = getDateTimeFormat();
        } else {
            theFormat = getDateFormat();
        }
        return toString(theDate, theFormat);
    }

    /**
     * 标准日期格式
     */
    private static final SimpleDateFormat DATE_FORMAT = new SimpleDateFormat("MM/dd/yyyy");
    /**
     * 标准时间格式
     */
    private static final SimpleDateFormat DATE_TIME_FORMAT = new SimpleDateFormat("MM/dd/yyyy HH:mm");
    /**
     * 带时分秒的标准时间格式
     */
    // private static final SimpleDateFormat DATE_TIME_EXTENDED_FORMAT = new
    // SimpleDateFormat("MM/dd/yyyy HH:mm:ss");
    /**
     * ORA标准日期格式
     */
    private static final SimpleDateFormat ORA_DATE_FORMAT = new SimpleDateFormat("yyyyMMdd");
    /**
     * ORA标准时间格式
     */
    private static final SimpleDateFormat ORA_DATE_TIME_FORMAT = new SimpleDateFormat("yyyyMMddHHmm");

    /**
     * 带时分秒的ORA标准时间格式
     */
    // private static final SimpleDateFormat ORA_DATE_TIME_EXTENDED_FORMAT = new
    // SimpleDateFormat("yyyyMMddHHmmss");

    /**
     * 创建一个标准日期格式的克隆
     *
     * @return 标准日期格式的克隆
     */
    public static DateFormat getDateFormat() {
        /**
         * 详细设计： 1.返回DATE_FORMAT
         */
        final SimpleDateFormat theDateFormat = (SimpleDateFormat) DATE_FORMAT.clone();
        theDateFormat.setLenient(false);
        return theDateFormat;
    }

    /**
     * 创建一个标准时间格式的克隆
     *
     * @return 标准时间格式的克隆
     */
    public static DateFormat getDateTimeFormat() {
        /**
         * 详细设计： 1.返回DATE_TIME_FORMAT
         */
        final SimpleDateFormat theDateTimeFormat = (SimpleDateFormat) DATE_TIME_FORMAT.clone();
        theDateTimeFormat.setLenient(false);
        return theDateTimeFormat;
    }

    /**
     * 创建一个标准ORA日期格式的克隆
     *
     * @return 标准ORA日期格式的克隆
     */
    public static DateFormat getOraDateFormat() {
        /**
         * 详细设计： 1.返回ORA_DATE_FORMAT
         */
        final SimpleDateFormat theDateFormat = (SimpleDateFormat) ORA_DATE_FORMAT.clone();
        theDateFormat.setLenient(false);
        return theDateFormat;
    }

    /**
     * 创建一个标准ORA时间格式的克隆
     *
     * @return 标准ORA时间格式的克隆
     */
    public static DateFormat getOraDateTimeFormat() {
        /**
         * 详细设计： 1.返回ORA_DATE_TIME_FORMAT
         */
        final SimpleDateFormat theDateTimeFormat = (SimpleDateFormat) ORA_DATE_TIME_FORMAT.clone();
        theDateTimeFormat.setLenient(false);
        return theDateTimeFormat;
    }

    /**
     * 将一个日期对象转换成为指定日期、时间格式的字符串。 如果日期对象为空，返回一个空字符串，而不是一个空对象。
     *
     * @param theDate       要转换的日期对象
     * @param theDateFormat 返回的日期字符串的格式
     * @return 转换结果
     */
    public static String toString(final Date theDate, final DateFormat theDateFormat) {
        /**
         * 详细设计： 1.theDate为空，则返回"" 2.否则使用theDateFormat格式化
         */
        if (theDate == null) return "";
        return theDateFormat.format(theDate);
    }

    /**
     * 得到日期
     *
     * @param y String
     * @param m String
     * @param d String
     * @return Date
     */
    public static Date getDate(final String y, final String m, final String d) {
        return getDate(Integer.parseInt(y), Integer.parseInt(m), Integer.parseInt(d));
    }

    public static Date getDate(final int y, final int m, final int d) {
        final java.util.Calendar cal = java.util.Calendar.getInstance();
        cal.set(Calendar.YEAR, y);
        cal.set(Calendar.MONTH, m - 1);
        cal.set(Calendar.DAY_OF_MONTH, d);
        return cal.getTime();
    }

    public static Date gotoLastSecond(final Date date) {
        final java.util.Calendar cal = java.util.Calendar.getInstance();
        cal.setTime(date);
        cal.set(Calendar.HOUR_OF_DAY, 23);
        cal.set(Calendar.MINUTE, 59);
        cal.set(Calendar.SECOND, 59);
        return cal.getTime();
    }

    public static Date gotoFirstSecond(final Date date) {
        final java.util.Calendar cal = java.util.Calendar.getInstance();
        cal.setTime(date);
        cal.set(Calendar.HOUR_OF_DAY, 0);
        cal.set(Calendar.MINUTE, 0);
        cal.set(Calendar.SECOND, 1);
        return cal.getTime();
    }

    public static final int UNIT_DAY = 0;
    public static final int UNIT_WEEK = 1;
    public static final int UNIT_MONTH = 2;
    public static final int UNIT_YEAR = 3;

    public static String getYyyymmdd(final Date date, final int count, final int type) {
        final Calendar cal = Calendar.getInstance();
        cal.setTime(date);
        if (type == UNIT_DAY) {
            cal.add(Calendar.DATE, count);
        } else if (type == UNIT_WEEK) {
            cal.add(Calendar.WEEK_OF_YEAR, count);
        } else if (type == UNIT_MONTH) {
            cal.add(Calendar.MONTH, count);
        } else if (type == UNIT_YEAR) {
            cal.add(Calendar.YEAR, count);
        }
        final int y = cal.get(Calendar.YEAR);
        final int m = cal.get(Calendar.MONTH) + 1;
        final int d = cal.get(Calendar.DAY_OF_MONTH);
        return y + twoDigits(m) + twoDigits(d);
    }

    public static String twoDigits(final int day) // 为了日历中数字能够对齐，所以1-9 前将加0
    {
        final String stringDay = String.valueOf(day); // 取得day的值
        if (stringDay.length() == 1) // 如果字符串长度为1
            return "0" + stringDay; // 则在字符串前加零
        return stringDay;
    }

    public static String getChinaNumber(final int num) {
        String ConvNumber = "";
        switch (num) {
            case 0:
                ConvNumber = "○";
                break;
            case 1:
                ConvNumber = "一";
                break;
            case 2:
                ConvNumber = "二";
                break;
            case 3:
                ConvNumber = "三";
                break;
            case 4:
                ConvNumber = "四";
                break;
            case 5:
                ConvNumber = "五";
                break;
            case 6:
                ConvNumber = "六";
                break;
            case 7:
                ConvNumber = "七";
                break;
            case 8:
                ConvNumber = "八";
                break;
            case 9:
                ConvNumber = "九";
                break;
            case 10:
                ConvNumber = "十";
                break;
            case 11:
                ConvNumber = "十一";
                break;
            case 12:
                ConvNumber = "十二";
                break;
            case 13:
                ConvNumber = "十三";
                break;
            case 14:
                ConvNumber = "十四";
                break;
            case 15:
                ConvNumber = "十五";
                break;
            case 16:
                ConvNumber = "十六";
                break;
            case 17:
                ConvNumber = "十七";
                break;
            case 18:
                ConvNumber = "十八";
                break;
            case 19:
                ConvNumber = "十九";
                break;
            case 20:
                ConvNumber = "二十";
                break;
            case 21:
                ConvNumber = "二十一";
                break;
            case 22:
                ConvNumber = "二十二";
                break;
            case 23:
                ConvNumber = "二十三";
                break;
            case 24:
                ConvNumber = "二十四";
                break;
            case 25:
                ConvNumber = "二十五";
                break;
            case 26:
                ConvNumber = "二十六";
                break;
            case 27:
                ConvNumber = "二十七";
                break;
            case 28:
                ConvNumber = "二十八";
                break;
            case 29:
                ConvNumber = "二十九";
                break;
            case 30:
                ConvNumber = "三十";
                break;
            case 31:
                ConvNumber = "三十一";
                break;
        }
        return ConvNumber;
    }

    /**
     * 获取中文格式的日期字符串
     *
     * @param date Date
     * @return String
     */
    public static String getChinaDataFormat(final Date date) {
        try {
            final Calendar cal = Calendar.getInstance();
            cal.setTime(date);
            final String y = Integer.toString(cal.get(Calendar.YEAR));
            final String m = Integer.toString(cal.get(Calendar.MONTH));
            final String d = Integer.toString(cal.get(Calendar.DAY_OF_MONTH));
            String result = "";
            for (int i = 0; i < y.length(); i++) {
                final int a = Integer.parseInt(y.substring(i, i + 1));
                result += getChinaNumber(a);
            }
            result += "年";
            result += getChinaNumber(Integer.parseInt(m) + 1) + "月";
            result += getChinaNumber(Integer.parseInt(d)) + "日";
            return result;
        } catch (final Exception e) {
            return "";
        }
    }

    /*
     * 将当前日期加减n天数。 如传入字符型"-5" 意为将当前日期减去5天的日期 如传入字符型"5" 意为将当前日期加上5天后的日期 返回字串
     * 例(1999-02-03)
     */
    public static String dateAdd(final String to) {
        // 日期处理模块 (将日期加上某些天或减去天数)返回字符串
        int strTo;
        try {
            strTo = Integer.parseInt(to);
        } catch (final Exception e) {
            System.out.println("日期标识转换出错! : \n:::" + to + "不能转为数字型");
            e.printStackTrace();
            strTo = 0;
        }
        final Calendar strDate = Calendar.getInstance(); // java.util包
        strDate.add(Calendar.DATE, strTo); // 日期减 如果不够减会将月变动
        // 生成 (年-月-日) 字符串
        final String meStrDate = strDate.get(Calendar.YEAR) + "-" + String.valueOf(strDate.get(Calendar.MONTH) + 1) + "-" + strDate.get(Calendar.DATE);
        return meStrDate;
    }

    /*
     * 将日期date加减n天数。 如传入字符型"-5" 意为将当前日期减去5天的日期 如传入字符型"5" 意为将当前日期加上5天后的日期 返回DATE
     */
    public static Date dateAdd(final Date date, final String n) {
        // 日期处理模块 (将日期加上某些天或减去天数)返回字符串
        int strTo;
        try {
            strTo = Integer.parseInt(n);
        } catch (final Exception e) {
            System.out.println("日期标识转换出错! : \n:::" + n + "不能转为数字型");
            e.printStackTrace();
            strTo = 0;
        }
        final Calendar strDate = Calendar.getInstance(); // java.util包
        strDate.setTime(date);
        strDate.add(Calendar.DATE, strTo); // 日期减 如果不够减会将月变动
        return strDate.getTime();
    }

    /**
     * 返回当前日期是第几周
     *
     * @param date Date
     * @return int
     */
    public static int getCurrentWeek(final Date date) {
        final Calendar cal = Calendar.getInstance();
        cal.setTime(date);
        return cal.get(Calendar.WEEK_OF_YEAR);
    }

    public static int getCurrentWeekOnMonth(final Date date) {
        final Calendar cal = Calendar.getInstance();
        cal.setTime(date);
        return cal.get(Calendar.WEEK_OF_MONTH);
    }

    /**
     * 获取某一年的周数
     *
     * @param y int
     * @return int
     */
    public static int getWeekCountOfYear(final int y) {
        final Calendar cal = Calendar.getInstance();
        cal.set(Calendar.YEAR, y);
        return cal.getMaximum(Calendar.WEEK_OF_YEAR);
    }

    /**
     * 获取某年某一周的第一天
     *
     * @param weekno int
     * @return Date
     */
    public static Date getFirstDayDateOfWeekNo(final int y, final int weekno) {
        final Calendar cal = Calendar.getInstance();
        cal.set(Calendar.YEAR, y);
        cal.set(Calendar.WEEK_OF_YEAR, weekno);
        return getFirstDayOfWeek(cal.getTime());
    }

    public static Date[] getStartAndEndDateOfDay(final Date date) {
        final Calendar cal = Calendar.getInstance();
        cal.setTime(date);
        final Date[] result = new Date[2];
        cal.set(Calendar.HOUR, 0);
        cal.set(Calendar.MINUTE, 0);
        cal.set(Calendar.SECOND, 0);
        cal.set(Calendar.MILLISECOND, 0);
        result[0] = cal.getTime();
        cal.set(Calendar.HOUR, 23);
        cal.set(Calendar.MINUTE, 59);
        cal.set(Calendar.SECOND, 59);
        result[1] = cal.getTime();
        return result;
    }

    public static Date[] getStartAndEndDateOfWeek(final Date date) {
        final Calendar cal = Calendar.getInstance();
        cal.setTime(date);
        final Date[] result = new Date[2];
        final int firstDay = cal.getMinimum(Calendar.DAY_OF_WEEK);
        final int lastDay = cal.getMaximum(Calendar.DAY_OF_WEEK);
        cal.set(Calendar.DAY_OF_WEEK, firstDay);
        cal.set(Calendar.HOUR_OF_DAY, 0);
        cal.set(Calendar.MINUTE, 0);
        cal.set(Calendar.SECOND, 0);
        result[0] = cal.getTime();
        cal.set(Calendar.DAY_OF_WEEK, lastDay);
        cal.set(Calendar.HOUR_OF_DAY, 23);
        cal.set(Calendar.MINUTE, 59);
        cal.set(Calendar.SECOND, 59);
        result[1] = cal.getTime();
        return result;
    }

    public static Date[] getStartAndEndDateOfMonth(final Date date) {
        final Calendar cal = Calendar.getInstance();
        cal.setTime(date);
        final Date[] result = new Date[2];
        final int firstDay = cal.getMinimum(Calendar.DAY_OF_MONTH);
        final int lastDay = cal.getMaximum(Calendar.DAY_OF_MONTH);
        cal.set(Calendar.DAY_OF_MONTH, firstDay);
        cal.set(Calendar.HOUR_OF_DAY, 0);
        cal.set(Calendar.MINUTE, 0);
        cal.set(Calendar.SECOND, 0);
        result[0] = cal.getTime();
        cal.set(Calendar.DAY_OF_MONTH, lastDay);
        cal.set(Calendar.HOUR_OF_DAY, 23);
        cal.set(Calendar.MINUTE, 59);
        cal.set(Calendar.SECOND, 59);
        result[1] = cal.getTime();
        return result;
    }

    /**
     * 获取一个时间段内的年列表,
     *
     * @param startTime
     * @param endTime
     * @return
     * @author Fei
     */
    public static List<Integer> getYearList(final Date startTime, final Date endTime) {
        final Calendar cal = Calendar.getInstance();
        cal.setTime(startTime);
        final int syear = cal.get(Calendar.YEAR);// start year
        cal.setTime(endTime);
        final int eyear = cal.get(Calendar.YEAR);// end year
        final List<Integer> yearList = new ArrayList<Integer>();
        for (int year = syear; year < eyear + 1; year++) {
            yearList.add(year);
        }
        return yearList;
    }

    /**
     * 获取当前时间属于那年
     *
     * @param date
     * @return
     */
    public static String getYear(Date date) {
        if (null == date) date = new Date();
        return getDateFormat(date, "yyyy");
    }

    /**
     * 两个日期相差天数计算
     *
     * @param startDate 开始时间
     * @param endDate   结束时间
     * @return
     */
    public static int getDifferDate(final String startDate, final String endDate) {
        final Date start = parseDateFormat(startDate, "yyyy-MM-dd");
        final Date end = parseDateFormat(endDate, "yyyy-MM-dd");
        return getDifferDate(start, end);
    }

    public static int getDifferDate(final Date startDate, final Date endDate) {
        // return getDifferDate(getDateFormat(startDate, "yyyy-MM-dd"),
        // getDateFormat(endDate, "yyyy-MM-dd"));
        final long startTime = startDate.getTime();
        final long endTime = endDate.getTime();
        return Math.abs((int) ((endTime - startTime) / DateUtils.DAY));
    }

    public static List<String> getDifferPrefix(final String startDate, final String endDate) {
        return getDifferPrefix(startDate, endDate, "yyMMdd");
    }

    /**
     * 获取两个指定日期之间的日期列表
     *
     * @param startDate
     * @param endDate
     * @param pattern
     * @return
     */
    public static List<String> getDifferPrefix(final String startDate, final String endDate, final String pattern) {
        final List<String> tablePrefix = new ArrayList<String>();
        final Date start = parseDateFormat(startDate, "yyyy-MM-dd");
        final Date end = parseDateFormat(endDate, "yyyy-MM-dd");
        final GregorianCalendar startCalendar = new GregorianCalendar();
        final GregorianCalendar endCalendar = new GregorianCalendar();
        startCalendar.setTime(start);
        endCalendar.setTime(end);
        while (startCalendar.before(endCalendar)) {
            tablePrefix.add(getDateFormat(startCalendar, pattern));
            startCalendar.add(Calendar.DATE, 1);
        }
        tablePrefix.add(getDateFormat(endCalendar, pattern));
        return tablePrefix;
    }

    public static String getShortYearDate(final String date) {
        final Date start = parseDateFormat(date, "yyyy-MM-dd");
        return getDateFormat(start, "yyMMdd");
    }

    public static String getLongYearDate(final String date) {
        final Date start = parseDateFormat(date, "yyyy-MM-dd");
        return getDateFormat(start, "yyyyMMdd");
    }

    public static String[] getDateFormat(final String date) {
        return getDateFormat(date, "yyyy-MM-dd HH");
    }

    public static String[] getDateFormat(final String date, final String pattern) {
        final String[] obj = new String[4];
        final Date start = parseDateFormat(date, pattern);
        obj[0] = getDateFormat(start, "yyyyMMdd");
        obj[1] = getDateFormat(start, "yyMMdd");
        obj[2] = getDateFormat(start, "HH");
        obj[3] = getDateFormat(start, "yyyy-MM-dd");
        return obj;
    }


}