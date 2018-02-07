package com.carpo.spark.utils;

import java.util.Collection;
import java.util.Iterator;
import java.util.Map;

/**
 * Author 李岩飞
 * Email eliyanfei@126.com
 * 2018/2/4
 */
public class StringsUtils {
    /**
     * 字符串是否为空
     *
     * @param str
     * @return
     */
    public static boolean isEmpty(String str) {
        return str == null || str.equals("");
    }

    public static boolean isNotEmpty(String str) {
        return !isEmpty(str);
    }

    public static boolean isEmpty(Object str) {
        return str == null || isEmpty((String) str);
    }

    public static String join(Collection<?> coll, String delim) {
        return join(coll, delim, false);
    }

    public static String join(Collection<?> coll, String delim, boolean str) {
        return coll != null && coll.size() >= 1 ? joinArray(coll.toArray(), delim) : null;
    }

    public static String joinArray(Object[] strArray, String delim) {
        return joinArray(strArray, delim, false);
    }

    public static String joinArray(Object[] strArray, String delim, boolean str) {
        StringBuilder joinedBuf = new StringBuilder();
        boolean isFirst = true;
        Object[] var8 = strArray;
        int var7 = strArray.length;

        for (int var6 = 0; var6 < var7; ++var6) {
            Object s = var8[var6];
            if (isFirst) {
                isFirst = false;
            } else {
                joinedBuf.append(delim);
            }

            if (str) {
                joinedBuf.append("\'");
            }

            joinedBuf.append(s);
            if (str) {
                joinedBuf.append("\'");
            }
        }

        return joinedBuf.toString();
    }

    public static final String u(Object... args) {
        if (args == null) {
            return "";
        } else if (args.length == 1) {
            return String.valueOf(args[0]);
        } else {
            StringBuilder buf = new StringBuilder();
            Object[] var5 = args;
            int var4 = args.length;

            for (int var3 = 0; var3 < var4; ++var3) {
                Object obj = var5[var3];
                buf.append(String.valueOf(obj));
            }

            return buf.toString();
        }
    }

    public static final String replace(String str, Map<String, Object> dataMap, String start, String end) {
        String key;
        for (Iterator var5 = dataMap.keySet().iterator(); var5.hasNext(); str = str.replace(start + key + end, String.valueOf(dataMap.get(key)))) {
            key = (String) var5.next();
        }

        return str;
    }

    public static String trimNull(final Object src, final String defValue) {
        return trimNull(String.valueOf(src), defValue);
    }

    public static String trimNull(final String src, final String defValue) {
        return trimNullByWEB(src, defValue);
    }

    public static String trimNull(final String src) {
        return trimNullByWEB(src, "");
    }

    /**
     * 判断字符串是否有内容。有内容意味着字符串不能为空且长度不为零。
     *
     * @param s
     *            需要判断的字符串
     * @return 有内容则返回<code>true</code>，否则返回<code>false</code>
     */
    public static final boolean hasContent(final String s) {
        return s != null && s.trim().length() > 0;
    }

    /**
     * 用于在WEB上对一个可能是null的字符串进行处理,使显示正常(不显示"null"之类在页面上)
     *
     * @param src
     *            需要进行处理的源字符串
     * @return 处理过的字符串
     */
    public static String trimNullByWEB(final String src) {
        return trimNullByWEB(src, "&nbsp;");
    }

    /**
     * 用于在WEB上对一个可能是null的字符串进行处理,使显示正常(不显示"null"之类在页面上),并传入一个默认值,
     * 如果对src进行更改的结果是一个空值,返回该传入的默认值
     *
     * @param src
     *            需要进行处理的源字符串
     * @param defaultValue
     *            用于第一个参数为空时返回的默认值
     * @return 处理过的字符串
     */
    public static String trimNullByWEB(final String src, final String defaultValue) {
        if (isEmpty(src))
            return defaultValue;

        String newValue = null;
        try {
            newValue = src.trim();
        } catch (final NullPointerException npe) {
            newValue = null;
        }

        if (null == newValue || "null".equals(newValue.toLowerCase()))
            return defaultValue;

        return newValue;
    }
}
