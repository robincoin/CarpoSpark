package com.carpo.spark.utils;


import java.text.DecimalFormat;
import java.text.NumberFormat;
import java.util.Locale;

/**
 * Author 李岩飞
 * Email eliyanfei@126.com
 * 2018/2/6
 */
public class NumberUtils {
    public static String formatNumber(final String format, final double fz, final double fm) {
        return formatNumber(format, fm == 0 ? 0 : fz / fm, Locale.getDefault());
    }

    public static double formatFmZero(final double fz, final double fm) {
        return fm == 0 ? 0 : fz / fm;
    }

    public static double formatFmZero(final double fz, final double fm, double defaultValue) {
        return fm == 0 ? defaultValue : fz / fm;
    }

    public static String formatNumber(final String format, final Object value) {
        return formatNumber(format, value, Locale.getDefault());
    }

    public static String formatNumber(final String format, final Object value, final Locale locale) {
        if (value == null)
            return null;
        if (StringsUtils.isEmpty(format))
            return value.toString();

        final NumberFormat nf = NumberFormat.getNumberInstance(locale);
        final DecimalFormat df = (DecimalFormat) nf;
        df.applyLocalizedPattern(format);
        try {
            return df.format(Double.parseDouble(value.toString()));
        } catch (final Exception e) {
            return value.toString();
        }
    }

    public static final Integer toInt(final Object obj) {
        if (obj == null) {
            return null;
        }

        try {
            if (obj instanceof Number) {
                return new Integer(((Number) obj).intValue());
            }
            if (obj instanceof Boolean) {
                return obj.equals(Boolean.FALSE) ? new Integer(0) : new Integer(-1);
            }

            return Integer.valueOf(obj.toString().trim());
        } catch (final Throwable t) {
            try {
                return Integer.valueOf(obj.toString().trim());
            } catch (final Throwable ta) {
                // ignore
            }
        }
        return null;
    }

    public static final int[] toInt(final String[] arr) {
        final int[] ii = new int[arr.length];
        for (int i = 0; i < ii.length; i++) {
            ii[i] = Integer.parseInt(arr[i]);
        }
        return ii;
    }

    public static final int toInt(final Object obj, final int defaultValue) {
        if (obj == null)
            return defaultValue;
        try {
            if (obj instanceof Number)
                return ((Number) obj).intValue();
            if (obj instanceof Boolean)
                return (Boolean) obj ? 0 : -1;
            return Integer.parseInt(obj.toString());
        } catch (final Throwable t) {
            return defaultValue;
        }
    }

    public static final Long toLong(final Object obj) {
        if (obj instanceof Long) {
            return (Long) obj;
        }
        if (obj instanceof Number) {
            return new Long(((Number) obj).longValue());
        }
        if (obj instanceof java.util.Date) {
            return new Long(((java.util.Date) obj).getTime());
        }
        if (obj instanceof java.sql.Timestamp) {
            return new Long(((java.sql.Timestamp) obj).getTime());
        }
        try {
            return new Long(Long.parseLong(obj.toString()));
        } catch (final Throwable t) {
        }
        return null;
    }

    public static final long toLong(final Object obj, final long defaultValue) {
        final Long l = toLong(obj);
        return l == null ? defaultValue : l.longValue();
    }

    public static final Double toDouble(final Object obj) {
        if (obj == null) {
            return null;
        }
        try {
            if (obj instanceof Number) {
                return new Double(((Number) obj).doubleValue());
            }
            if (obj instanceof Boolean) {
                return obj.equals(Boolean.FALSE) ? new Double(0.0) : new Double(-1.0);
            }

            return Double.valueOf(obj.toString());
        } catch (final Throwable t) {
        }
        return Double.valueOf(0);
    }

    public static final double toDouble(final Object obj, final double defaultValue) {
        final Double d = toDouble(obj);
        return d == null ? defaultValue : d.doubleValue();
    }

    public static final float toFloat(final Object obj, final float defauleValue) {
        return (float) toDouble(obj, defauleValue);
    }

    public static final Byte toByte(final Object obj) {
        if (obj == null) {
            return null;
        }
        if (obj instanceof Number) {
            return new Byte(((Number) obj).byteValue());
        }
        if (obj instanceof Boolean) {
            return obj.equals(Boolean.FALSE) ? new Byte((byte) 0) : new Byte((byte) -1);
        }
        try {
            return Byte.valueOf(obj.toString());
        } catch (final Throwable t) {
        }
        return null;
    }

    public static final byte toByte(final Object obj, final byte defaultValue) {
        if (obj == null)
            return defaultValue;
        try {
            if (obj instanceof Number)
                return ((Number) obj).byteValue();
            if (obj instanceof Boolean)
                return (byte) ((Boolean) obj ? 0 : -1);
            return Byte.parseByte(obj.toString());
        } catch (final Throwable t) {
            return defaultValue;
        }
    }

    public static final Short toShort(final Object obj) {
        if (obj == null) {
            return null;
        }
        if (obj instanceof Number) {
            return new Short(((Number) obj).shortValue());
        }
        if (obj instanceof Boolean) {
            return obj.equals(Boolean.FALSE) ? new Short((short) 0) : new Short((short) -1);
        }
        try {
            return Short.valueOf(obj.toString());
        } catch (final Throwable t) {
        }
        return null;
    }

    public static final short toShort(final Object obj, final short defaultValue) {
        if (obj == null)
            return defaultValue;
        try {
            if (obj instanceof Number)
                return ((Number) obj).shortValue();
            if (obj instanceof Boolean)
                return (short) ((Boolean) obj ? 0 : -1);
            return Short.parseShort(obj.toString());
        } catch (final Throwable t) {
            return defaultValue;
        }
    }

    public static final Boolean toBoolean(final Object obj) {
        if (obj == null) {
            return null;
        }
        if (obj instanceof Boolean) {
            return (Boolean) obj;
        } else if (obj instanceof Number) {
            return ((Number) obj).intValue() == 0 ? Boolean.FALSE : Boolean.TRUE;
        } else if (obj instanceof String) {
            final String s = (String) obj;
            if (s.equalsIgnoreCase("true")) {
                return Boolean.TRUE;
            } else if (s.equalsIgnoreCase("false")) {
                return Boolean.FALSE;
            } else {
                try {
                    return new Boolean(Integer.parseInt((String) obj) != 0);
                } catch (final Throwable t) {
                    return Boolean.FALSE;
                }
            }
        }
        return null;
    }

    public static final boolean toBoolean(final Object value, final boolean defaultValue) {
        final Boolean v = toBoolean(value);
        return v == null ? defaultValue : v.booleanValue();
    }
}
