package org.apache.solr.handler.component.aggregates;

import java.text.ParseException;
import java.util.TimeZone;
import java.util.regex.Pattern;

import org.apache.solr.common.SolrException;
import org.apache.solr.common.params.CommonParams;
import org.apache.solr.request.SolrRequestInfo;
import org.joda.time.DateTime;
import org.joda.time.format.ISODateTimeFormat;

/**
 * A Simple Utility class for parsing "math" like strings relating to Dates.
 * 
 * <p>
 * The basic syntax support addition, subtraction and rounding at various levels of granularity (or
 * "units"). Commands can be chained together and are parsed from left to right. '+' and '-' denote
 * addition and subtraction, while '/' denotes "round". Round requires only a unit, while
 * addition/subtraction require an integer value and a unit. Command strings must not include white
 * space, but the "No-Op" command (empty string) is allowed....
 * </p>
 * 
 * <pre>
 *   /HOUR
 *      ... Round to the start of the current hour
 *   /DAY
 *      ... Round to the start of the current day
 *   +2YEARS
 *      ... Exactly two years in the future from now
 *   -1DAY
 *      ... Exactly 1 day prior to now
 *   /DAY+6MONTHS+3DAYS
 *      ... 6 months and 3 days in the future from the start of
 *          the current day
 *   +6MONTHS+3DAYS/DAY
 *      ... 6 months and 3 days in the future from now, rounded
 *          down to nearest day
 * </pre>
 * 
 * <p>
 * (Multiple aliases exist for the various units of time (ie: <code>MINUTE</code> and
 * <code>MINUTES</code>; <code>MILLI</code>, <code>MILLIS</code>, <code>MILLISECOND</code>, and
 * <code>MILLISECONDS</code>.) The complete list can be found by inspecting the keySet of
 * {@link #CALENDAR_UNITS})
 * </p>
 * 
 * <p>
 * All commands are relative to a "now" which is fixed in an instance of DateMathParser such that
 * <code>p.parseMath("+0MILLISECOND").equals(p.parseMath("+0MILLISECOND"))</code> no matter how many
 * wall clock milliseconds elapse between the two distinct calls to parse (Assuming no other thread
 * calls "<code>setNow</code>" in the interim). The default value of 'now' is the time at the moment
 * the <code>DateMathParser</code> instance is constructed, unless overridden by the
 * {@link CommonParams#NOW NOW} request param.
 * </p>
 * 
 * <p>
 * All commands are also affected to the rules of a specified {@link TimeZone} (including the
 * start/end of DST if any) which determine when each arbitrary day starts. This not only impacts
 * rounding/adding of DAYs, but also cascades to rounding of HOUR, MIN, MONTH, YEAR as well. The
 * default <code>TimeZone</code> used is <code>UTC</code> unless overridden by the
 * {@link CommonParams#TZ TZ} request param.
 * </p>
 * 
 * @see SolrRequestInfo#getClientTimeZone
 * @see SolrRequestInfo#getNOW
 */
public class DateMathParserFixed {

    private static final String NOW = "NOW";

    private static final String Z = "Z";

    public static String toIsoFormat(DateTime dt) {
        return dt.toString(ISODateTimeFormat.dateTimeNoMillis());
    }

    public static DateTime fromIsoFormat(String dt) {
        return DateTime.parse(dt);
    }

    public static DateTime extract(DateTime now, String val) {
        String math = null;
        final DateMathParserFixed p = new DateMathParserFixed();

        if (null != now)
            p.setNow(now);

        if (val.startsWith(NOW)) {
            math = val.substring(NOW.length());
        } else {
            final int zz = val.indexOf(Z);
            if (0 < zz) {
                math = val.substring(zz + 1);
                String x = val.substring(0, zz + 1);
                DateTime dt = DateTime.parse(x);
                p.setNow(dt);

            } else {
                throw new SolrException(SolrException.ErrorCode.BAD_REQUEST, "Invalid Date String:'" + val + '\'');
            }
        }

        if (null == math || math.equals("")) {
            return p.getNow();
        }

        try {
            return p.parseMath(math);
        } catch (ParseException e) {
            throw new SolrException(SolrException.ErrorCode.BAD_REQUEST, "Invalid Date Math String:'" + val + '\'', e);
        }
    }

    /**
     * Modifies the specified Calendar by "adding" the specified value of units
     * 
     * @exception IllegalArgumentException
     *                if unit isn't recognized.
     * @see #CALENDAR_UNITS
     */
    public static DateTime add(DateTime c, int val, String unit) {
        if (unit.equalsIgnoreCase("YEAR")) {
            return c.plusYears(val);
        }
        if (unit.equalsIgnoreCase("MONTH")) {
            return c.plusMonths(val);
        }
        if (unit.equalsIgnoreCase("DAY")) {
            return c.plusDays(val);
        }
        if (unit.equalsIgnoreCase("HOUR")) {
            return c.plusHours(val);
        }
        if (unit.equalsIgnoreCase("MINUTE")) {
            return c.plusMinutes(val);
        }
        if (unit.equalsIgnoreCase("SECOND")) {
            return c.plusSeconds(val);
        }
        if (unit.equalsIgnoreCase("WEEK")) {
            return c.plusWeeks(val);
        }
        throw new IllegalArgumentException("Adding Unit not recognized: " + unit);
    }

    /**
     * Modifies the specified Calendar by "rounding" down to the specified unit
     * 
     * @exception IllegalArgumentException
     *                if unit isn't recognized.
     * @see #CALENDAR_UNITS
     */
    public static DateTime round(DateTime c, String unit) {
        if (unit.equalsIgnoreCase("YEAR")) {
            return c.withTimeAtStartOfDay().withMonthOfYear(1);
        }
        if (unit.equalsIgnoreCase("MONTH")) {
            return c.withTimeAtStartOfDay().withDayOfMonth(1);
        }
        if (unit.equalsIgnoreCase("DAY")) {
            return c.withTimeAtStartOfDay();
        }
        if (unit.equalsIgnoreCase("HOUR")) {
            return c.withMinuteOfHour(0).withSecondOfMinute(0).withMillis(0);
        }
        if (unit.equalsIgnoreCase("MINUTE")) {
            return c.withSecondOfMinute(0).withMillis(0);
        }
        if (unit.equalsIgnoreCase("SECOND")) {
            return c.withMillis(0);
        }
        if (unit.equalsIgnoreCase("WEEK")) {
            return c.withDayOfWeek(1);
        }
        System.out.println("Rounding Unit not recognized: " + unit);
        throw new IllegalArgumentException("Rounding Unit not recognized: " + unit);

    }

    private DateTime now;

    /**
     * Default constructor that assumes UTC should be used for rounding unless otherwise specified
     * in the SolrRequestInfo
     * 
     * @see SolrRequestInfo#getClientTimeZone
     * @see #DEFAULT_MATH_LOCALE
     */
    public DateMathParserFixed() {

    }

    /**
     * Defines this instance's concept of "now".
     * 
     * @see #getNow
     */
    public void setNow(DateTime n) {
        now = n;
    }

    /**
     * Returns a cloned of this instance's concept of "now".
     * 
     * If setNow was never called (or if null was specified) then this method first defines 'now' as
     * the value dictated by the SolrRequestInfo if it exists -- otherwise it uses a new Date
     * instance at the moment getNow() is first called.
     * 
     * @see #setNow
     * @see SolrRequestInfo#getNOW
     */
    public DateTime getNow() {
        if (now == null) {
            return new DateTime();
        }
        return new DateTime(now);
    }

    /**
     * Parses a string of commands relative "now" are returns the resulting Date.
     * 
     * @exception ParseException
     *                positions in ParseExceptions are token positions, not character positions.
     */
    public DateTime parseMath(String math) throws ParseException {

        DateTime dt = getNow();

        /* check for No-Op */
        if (0 == math.length()) {
            return dt;
        }

        String[] ops = splitter.split(math);
        int pos = 0;
        while (pos < ops.length) {

            if (1 != ops[pos].length()) {
                throw new ParseException("Multi character command found: \"" + ops[pos] + "\"", pos);
            }
            char command = ops[pos++].charAt(0);

            switch (command) {
                case '/':
                    if (ops.length < pos + 1) {
                        throw new ParseException("Need a unit after command: \"" + command + "\"", pos);
                    }
                    try {
                        dt = round(dt, ops[pos++]);
                    } catch (IllegalArgumentException e) {
                        throw new ParseException("Unit not recognized: \"" + ops[pos - 1] + "\"", pos - 1);
                    }
                    break;
                case '+': /* fall through */
                case '-':
                    if (ops.length < pos + 2) {
                        throw new ParseException("Need a value and unit for command: \"" + command + "\"", pos);
                    }
                    int val = 0;
                    try {
                        val = Integer.valueOf(ops[pos++]);
                    } catch (NumberFormatException e) {
                        throw new ParseException("Not a Number: \"" + ops[pos - 1] + "\"", pos - 1);
                    }
                    if ('-' == command) {
                        val = 0 - val;
                    }
                    try {
                        String unit = ops[pos++];
                        dt = add(dt, val, unit);
                    } catch (IllegalArgumentException e) {
                        throw new ParseException("Unit not recognized: \"" + ops[pos - 1] + "\"", pos - 1);
                    }
                    break;
                default:
                    throw new ParseException("Unrecognized command: \"" + command + "\"", pos - 1);
            }
        }

        return dt;
    }

    private static Pattern splitter = Pattern.compile("\\b|(?<=\\d)(?=\\D)");

}
