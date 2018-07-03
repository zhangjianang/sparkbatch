package bi;

import bi.postgresql.model.CoreConfig;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.Date;
import java.util.GregorianCalendar;

/** 
 * @ClassName:
 * @Description: 时间处理类 
 * @author wqf
 * @date Oct 26, 2011 10:40:39 AM 
 * 
 * @version 3.0.0 
 */
@SuppressWarnings("ALL")
public class TimeTool {
	private static String idate;

	public static String genDbInfo(String[] args) {
		StringBuilder hiveDataBase = new StringBuilder();
		if(args!=null&&args.length>0){
			idate = args[0];
		}else{
			Calendar calendar = Calendar.getInstance();//此时打印它获取的是系统当前时间
			calendar.add(Calendar.DATE, -1);    //得到前一天
			idate = new SimpleDateFormat("yyyyMMdd").format(calendar.getTime());
		}
		return hiveDataBase.append("use prism_").append(idate).toString();
	}

	public static String genHdfsUrl(){
		if(idate != null ){
			return CoreConfig.HDFS_URL+ idate+"/";
		}
		return "";
	}

	private static SimpleDateFormat dateFormat = new SimpleDateFormat(
			"yyyy-MM-dd HH:mm:ss");

	private static SimpleDateFormat format = new SimpleDateFormat(
			"yyyy-MM-dd'T'HH:mm:ss");


	private static SimpleDateFormat longformat = new SimpleDateFormat(
			"yyyy-MM-dd HH:mm:ss,SSS");


	public static String getLongDate(){
		return longformat.format(new Date());
	}

	public static String getTimeFormat(long currentTime ){
		if(currentTime==0){
			return "";
		}
		return format.format(currentTime);
	}
	
	public static String getCurLongDate(long l, String s) {
		return dateFormat.format(new Date());
	}
	
	

	public static String getCurDate() {
		return dateFormat.format(new Date()).substring(0, 10);
	}

	public static boolean isDate(String date) {
		try {
			dateFormat.parse(date);
			return true;
		} catch (ParseException e) {
			return false;
		}
	}

	public static int dateComp(String Date1, String Date2) {
		if ((Date1 == null || Date1.trim().length() == 0)
				&& (Date2 == null || Date2.trim().length() == 0))
			return 0;

		if (Date2 == null || Date2.trim().length() == 0)
			return 1;

		if (Date1 == null || Date1.trim().length() == 0)
			return -1;

		if (!isDate(Date1) || !isDate(Date2))
			return 0;

		String sD1 = java.sql.Date.valueOf(Date1).toString();
		String sD2 = java.sql.Date.valueOf(Date2).toString();

		return sD1.compareTo(sD2);
	}

	/**
	 * 移动日期（天）。
	 * 
	 * 创建日期：(2001-6-28 15:57:34)
	 * 
	 * @return java.lang.String 
	 * @param sCurDate
	 *            java.lang.String
	 * @param iDays
	 *            int
	 */
	public static String dateMove(String sCurDate, int iDays) {
		long lD = java.sql.Date.valueOf(sCurDate).getTime();
		long ll = 1;
		ll = ll * iDays * 24 * 3600 * 1000;
		lD = lD + ll;
		return new java.sql.Date(lD).toString();
	}

	/**
	 * 移动日
	 * @param mtime
	 * @param moveDay
	 * @return
	 */
	public static long dateMove(long mtime, int moveDay) {
		Calendar c = getCal(mtime);
		c.add(Calendar.DAY_OF_MONTH, moveDay);
		return c.getTimeInMillis();
	}

	/**
	 * 如果移动后的月没有相同日子 比如3月31日 移动-1月 那么将返回2月28号 即移动后月的最后一日
	 * @param mtime
	 * @param moveMonth
	 * @return
	 */
	public static long monthMove(long mtime, int moveMonth) {
		Calendar c = getCal(mtime);
		c.add(Calendar.MONTH, moveMonth);
		return c.getTimeInMillis();
	}

	public static long hourMove(long mtime, int moveHour) {
		Calendar c = getCal(mtime);
		c.add(Calendar.HOUR_OF_DAY, moveHour);
		return c.getTimeInMillis();
	}

	public static long minuteMove(long mtime, int moveMinute) {
		Calendar c = getCal(mtime);
		c.add(Calendar.MINUTE, moveMinute);
		return c.getTimeInMillis();
	}

	public static long secondMove(long mtime, int moveSecond) {
		Calendar c = getCal(mtime);
		c.add(Calendar.SECOND, moveSecond);
		return c.getTimeInMillis();
	}

	public static long yearMove(long mtime, int moveYear) {
		Calendar c = getCal(mtime);
		c.add(Calendar.YEAR, moveYear);
		return c.getTimeInMillis();
	}

	public static Calendar getCal(long mtime) {
		Calendar c = Calendar.getInstance();
		c.setTimeInMillis(mtime);
		return c;
	}

	public static long timeMove(long mtime, int moveYear, int moveMonth,
			int moveDay, int moveHour, int moveMinute, int moveSecond) {
		long r = yearMove(mtime, moveYear);
		r = monthMove(r, moveMonth);
		r = dateMove(r, moveDay);
		r = hourMove(r, moveHour);
		r = minuteMove(r, moveMinute);
		r = secondMove(r, moveSecond);

		return r;
	}

	public static long daySub(long time1, long time2) {
		long lD1 = time1;
		long lD2 = time2;
		long ll = lD1 - lD2;
		ll = ll / (24 * 3600 * 1000);
		return ll;
	}

	/**
	 * 计算天数。 创建日期：(2001-6-28 15:57:34)
	 * 
	 * @return java.lang.String
	 * @param
	 *
	 * @param
	 *
	 */
	public static long daySub(String Date1, String Date2) {
		return daySub(java.sql.Date.valueOf(Date1), java.sql.Date
				.valueOf(Date2));
	}

	/**
	 * 计算天数。 创建日期：(2001-6-28 15:57:34)
	 * 
	 * @return java.lang.String
	 * @param
	 *
	 * @param
	 *
	 */
	public static long daySub(java.sql.Date Date1, java.sql.Date Date2) {
		return daySub(Date1.getTime(), Date2.getTime());
		// long lD1 = Date1.getTime();
		// long lD2 = Date2.getTime();
		// long ll =lD1-lD2;
		// ll=ll/(24*3600*1000);
		// return ll;
	}

	/**
	 * 取得当前日期。
	 * 
	 * 创建日期：(2001-6-28 11:00:03)
	 * 
	 * @return java.lang.String
	 */

	/**
	 * 取得当前天。
	 * 
	 * 创建日期：(2001-6-28 11:33:16)
	 * 
	 * @return java.lang.String
	 */
	public static String getCurDay() {
		String s = getCurDate();
		return s.substring(s.lastIndexOf("-") + 1);
	}

	/**
	 * 取得当前小时。 time 里有hour
	 */
	public static String getCurHour(String time) {
		if (time == null) {
			time = dateFormat.format(new Date());
		}
		int iB = time.indexOf(":");
		return time.substring(iB - 2, iB);
	}

	/**
	 * 取得当前月。
	 * 
	 * 创建日期：(2001-6-28 11:33:16)
	 * 
	 * @return java.lang.String
	 */
	public static String getCurMonth() {
		String s = getCurDate();
		int iB = s.indexOf("-");
		return s.substring(iB + 1, s.indexOf("-", iB + 1));
	}

	/**
	 * 取得当前时间。
	 * 
	 * 创建日期：(2001-6-28 11:00:03)
	 * 
	 * @return java.lang.String
	 */
	public static String getCurTime() {

		String st = new Date().toString();
		int iB = st.indexOf(":");
		return st.substring(iB - 2, iB + 6);
	}

	/**
	 * 取得当前周几。
	 * 
	 * 创建日期：(2001-6-28 11:33:16)
	 * 
	 * @return java.lang.String
	 */
	public static int getCurWeekDay() {
		String sCurDay = getCurDate();
		String sDay = "1000-01-07";// 周日
		return Integer.parseInt(daySub(sCurDay, sDay) % 7 + "");
	}

	/**
	 * 取得当前年。
	 * 
	 * 创建日期：(2001-6-28 11:33:16)
	 * 
	 * @return java.lang.String
	 */
	public static String getCurYear() {
		String s = getCurDate();
		return s.substring(0, s.indexOf("-"));
	}

	/**
	 * 取得当前天日期。
	 * 
	 * 创建日期：(2001-6-28 11:00:03)
	 * 
	 * @return java.lang.String
	 */
	public static String getDay(String sdate) {
		try {
			java.sql.Date dt = java.sql.Date.valueOf(sdate);
			return dt.toString().substring(8, 10);

		} catch (Exception e) {
			return "0";
		}
	}

	/**
	 * 取得本地当前日期。
	 * 
	 * 创建日期：(2001-6-28 11:00:03)
	 * 
	 * @return java.lang.String
	 */
	public static String getLocalCurDate() {
		String sData = getCurDate();
		return getYear(sData) + "年" + getMonth(sData) + "月" + getDay(sData)
				+ "日";
	}

	/**
	 * 周几。
	 * 
	 * 创建日期：(2001-6-28 11:00:03)
	 * 
	 * @return java.lang.String
	 */
	public static String getLocalCurWeekDay() {
		int iDay = getCurWeekDay();
		String sLocalDay = "";
		if (iDay == 0)
			sLocalDay = "日";
		else if (iDay == 1)
			sLocalDay = "一";
		else if (iDay == 2)
			sLocalDay = "二";
		else if (iDay == 3)
			sLocalDay = "三";
		else if (iDay == 4)
			sLocalDay = "四";
		else if (iDay == 5)
			sLocalDay = "五";
		else if (iDay == 6)
			sLocalDay = "六";

		return "星期" + sLocalDay;
	}

	/**
	 * 取得本地日期。
	 * 
	 * 创建日期：(2001-6-28 11:00:03)
	 * 
	 * @return java.lang.String
	 */
	public static String getLocalDate(String sDate) {
		return getYear(sDate) + "年" + getMonth(sDate) + "月" + getDay(sDate)
				+ "日";
	}

	/**
	 * 本地周几s。
	 * 
	 * 创建日期：(2001-6-28 11:00:03)
	 * 
	 * @return java.lang.String
	 */
	public static String getLocalWeekDay(String sDate) {
		int iDay = getWeekDay(sDate);
		String sLocalDay = "";
		if (iDay == 0)
			sLocalDay = "日";
		else if (iDay == 1)
			sLocalDay = "一";
		else if (iDay == 2)
			sLocalDay = "二";
		else if (iDay == 3)
			sLocalDay = "三";
		else if (iDay == 4)
			sLocalDay = "四";
		else if (iDay == 5)
			sLocalDay = "五";
		else if (iDay == 6)
			sLocalDay = "六";

		return "星期" + sLocalDay;
	}

	/**
	 * 取得当前月。
	 * 
	 * 创建日期：(2001-6-28 11:00:03)
	 * 
	 * @return java.lang.String
	 */
	public static String getMonth(String sdate) {
		try {
			java.sql.Date dt = java.sql.Date.valueOf(sdate);
			return dt.toString().substring(5, 7);

		} catch (Exception e) {
			return "0";
		}
	}
	
	/**
	 * 返回指定时间所在月的1日0点0分0秒0豪
	 * @param time
	 * @return
	 */
	public static long getMonthStart(long time){
		Calendar c = getCal(time);
		c.set(Calendar.DAY_OF_MONTH, 1);
		c.set(Calendar.HOUR_OF_DAY, 0);
		c.set(Calendar.MINUTE, 0);
		c.set(Calendar.SECOND, 0);
		c.set(Calendar.MILLISECOND, 0);
		
		return c.getTimeInMillis();
	}
	
	/**
	 * 返回指定时间所在日的0点0分0秒0豪
	 * @param time
	 * @return
	 */
	public static long getDayStart(long time){
		Calendar c = getCal(time);
		c.set(Calendar.HOUR_OF_DAY, 0);
		c.set(Calendar.MINUTE, 0);
		c.set(Calendar.SECOND, 0);
		c.set(Calendar.MILLISECOND, 0);
		
		return c.getTimeInMillis();
	}
	
	/**
	 * 取得指定时间所在小时的0分0秒0豪
	 * @param time
	 * @return
	 */
	public static long getHourStart(long time){
		Calendar c = getCal(time);
		c.set(Calendar.MINUTE, 0);
		c.set(Calendar.SECOND, 0);
		c.set(Calendar.MILLISECOND, 0);
		
		return c.getTimeInMillis();
	}

	
	/**
	 * 取得某个时刻所在月份的天数
	 * @param time
	 * @return
	 */
	public static int getNumOfMonth(long time){
		Calendar cal = getCal(time);
		cal.set(Calendar.DAY_OF_MONTH, 1);
		cal.add(Calendar.MONTH, 1);
		cal.add(Calendar.DAY_OF_MONTH, -1);
		return cal.get(Calendar.DAY_OF_MONTH);
	}
	
	/**
	 * 取得某月多少天 注意月份时1月就是1 而不是Calendar规定的1月是0！！！
	 * @param year
	 * @param month
	 * @return
	 */
	public static int getNumOfMonth(int year, int month){
		Calendar c = Calendar.getInstance();
		c.set(Calendar.DAY_OF_MONTH, 1);
		c.set(Calendar.YEAR, year);
		c.set(Calendar.MONTH, month);
		c.add(Calendar.DAY_OF_MONTH, -1);
		return c.get(Calendar.DAY_OF_MONTH);
	}

	/**
	 * 取得当前周几。
	 * 
	 * 创建日期：(2001-6-28 11:33:16)
	 * 
	 * @return java.lang.String
	 */
	public static int getWeekDay(String sDate) {
		String sDay = "1000-01-07";// 周日
		return Integer.parseInt(daySub(sDate, sDay) % 7 + "");
	}

	/**
	 * 取得年。
	 * 
	 * 创建日期：(2001-6-28 11:00:03)
	 * 
	 * @return java.lang.String
	 */
	public static String getYear(String sdate) {
		try {
			java.sql.Date dt = java.sql.Date.valueOf(sdate);
			return dt.toString().substring(0, 4);

		} catch (Exception e) {
			return "0";
		}
	}



	/**
	 * 是否闰年
	 * 
	 * 创建日期：(2001-9-13 14:28:31)
	 * 
	 * @return boolean
	 * @param sDate
	 *            java.lang.String
	 */
	public static boolean isRenYear(String sDate) {
		try {
			//String s = java.sql.Date.valueOf(sDate).toString().trim();
			//int iYear = StrTool.str2int(s.substring(0, 5));
			int iYear = getYear(getStrTime(sDate));

			if (iYear % 4 == 0) {
				if (iYear % 100 == 0 && iYear % 400 != 0) {
					return false;
				} else
					return true;
			}
			return false;
		} catch (Exception e) {
			return false;
		}

	}
	
	/**
	 * 判断某年是否闰年 注意！此方法输入的是年份
	 * @param iYear 所给年份
	 * @return
	 */
	public static boolean isRenYear(int iYear){
		return iYear % 4 == 0 && !(iYear % 100 == 0 && iYear % 400 != 0);
	}

	/**
	 * 判断某时刻所在年是否闰年
	 * @param time
	 * @return
	 */
	public static boolean isRenYearByLong(long time){
		return isRenYear(getCal(time).get(Calendar.YEAR));
	}

	/**
	 * 移动月。
	 * 
	 * 创建日期：(2001-6-28 15:57:34)
	 * 
	 * @return java.lang.String
	 * @param sDate
	 *            2001-02
	 * @param
	 *
	 */
	public static String MonthMove(String sDate, int iMonth) {
		int iyear = Integer.parseInt(sDate.substring(0, sDate.indexOf("-")));
		int imonth = Integer.parseInt(sDate.substring(sDate.indexOf("-") + 1,
				sDate.lastIndexOf("-"))) - 1;
		int iday = Integer
				.parseInt(sDate.substring(sDate.lastIndexOf("-") + 1));

		GregorianCalendar thedate = new GregorianCalendar(
				iyear, imonth, iday);
		thedate.add(GregorianCalendar.MONTH, iMonth);
		Date d = thedate.getTime();
		java.text.DateFormat df = java.text.DateFormat.getDateInstance();

		String s = df.format(d);

		String syear = s.substring(0, s.indexOf("-"));
		String smonth = s.substring(s.indexOf("-") + 1, s.lastIndexOf("-"));
		if (smonth.length() == 1)
			smonth = "0" + smonth;
		String sday = s.substring(s.lastIndexOf("-") + 1);
		if (sday.length() == 1)
			sday = "0" + sday;

		return syear + "-" + smonth + "-" + sday;
	}

	public static int getCurYearN() {
		return getCal(System.currentTimeMillis()).get(Calendar.YEAR);
	}

	/**
	 * 返回月 1月是1 2月是2...以此类推
	 * 
	 * @return
	 */
	public static int getCurMonthN() {
		return getCal(System.currentTimeMillis()).get(Calendar.MONTH) + 1;
	}

	public static int getCurDayOfMonthN() {
		return getCal(System.currentTimeMillis()).get(Calendar.DAY_OF_MONTH);
	}

	public static int getCurHourN() {
		return getCal(System.currentTimeMillis()).get(Calendar.HOUR_OF_DAY);
	}

	public static int getCurMinuteN() {
		return getCal(System.currentTimeMillis()).get(Calendar.MINUTE);
	}

	public static int getCurSecondN() {
		return getCal(System.currentTimeMillis()).get(Calendar.SECOND);
	}

	/**
	 * 当前毫秒
	 * 
	 * @return
	 */
	public static int getCurMillSecN() {
		return getCal(System.currentTimeMillis()).get(Calendar.MILLISECOND);
	}

	/**
	 * 时间转成指定格式
	 * <br>和getTimeStr函数功能一样
	 * @param dateFmt
	 * @param time
	 * @return
	 */
	public static String toFormat(String dateFmt, long time) {
		return new SimpleDateFormat(dateFmt).format(new Date(time));
	}

	/**
	 * 以指定格式解析
	 * 
	 * @param dateFmt
	 * @param
	 * @return
	 * @throws ParseException
	 */
	public static long parseDate(String dateFmt, String timeStr) throws ParseException {
		SimpleDateFormat sdf = new SimpleDateFormat(dateFmt);
		return sdf.parse(timeStr).getTime();
	}



	/**
	 * 创建一个"yyyy-MM-dd HH:mm:ss"格式的SimpleDateFormat
	 * 
	 * @return
	 */
	public static SimpleDateFormat new_yyyy_MM_dd_HH_mm_ss_FMT() {
		return new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
	}
	
	/**
	 * 创建一个"yyyy-MM-dd"格式的SimpleDateFormat
	 * @return
	 */
	public static SimpleDateFormat new_yyyy_MM_dd_FMT() {
		return new SimpleDateFormat("yyyy-MM-dd");
	}

	/**
	 * 给定一个"yyyy-MM-dd HH:mm:ss"格式或"yyyy-MM-dd HH:mm:ss"格式的时间 转换成long时间
	 * 
	 * @param
	 * @return
	 * @throws ParseException
	 */
	public static long getStrTime(String yyyy_MM_dd_HH_mm_ss__OR__yyyy_MM_dd) throws ParseException {
		try{
			return new_yyyy_MM_dd_HH_mm_ss_FMT().parse(yyyy_MM_dd_HH_mm_ss__OR__yyyy_MM_dd)
				.getTime();
		}catch (Exception e) {
			return new_yyyy_MM_dd_FMT().parse(yyyy_MM_dd_HH_mm_ss__OR__yyyy_MM_dd).getTime();
		}
	}
	
	/**
	 * 按照指定的格式解析时间
	 * <br>第一个参数是时间字符串 第二个是格式字符串
	 * @param formatedTime
	 * @param formatString
	 * @return
	 * @throws ParseException
	 */
	public static long getStrTime(String formatedTime, String formatString) throws ParseException{
		return new SimpleDateFormat(formatString).parse(formatedTime).getTime();
	}

	/**
	 * 给定一个time时间 转换成"yyyy-MM-dd HH:mm:ss"格式时间
	 * 
	 * @param time
	 * @return
	 */
	public static String getTimeStr(long time) {
		return new_yyyy_MM_dd_HH_mm_ss_FMT().format(new Date(time));
	}

	/**
	 * 返回某个时间的指定格式
	 * @param time
	 * @param dateFormat
	 * @return
	 */
	public static String getTimeStr(long time, String dateFormat){
		return new SimpleDateFormat(dateFormat).format(new Date(time));
	}
	
	/**
	 * 指定的时间所处月份 是否是 1 4 7 10月
	 * <br>即季度开始月
	 * @param time
	 * @return
	 */
	public static boolean isQuartStartMonth(long time) {
		Calendar curCalendar = Calendar.getInstance();
		curCalendar.setTimeInMillis(time);
		final int month = curCalendar.get(Calendar.MONTH);
		return month == 0 || month == 3 || month == 6 || month == 9;
	}

	/**
	 * 取得某个时间的年部分
	 * @param time
	 * @return
	 */
	public static int getYear(long time){
		Calendar cal = getCal(time);
		return cal.get(Calendar.YEAR);
	}
	
	/**
	 * 取得月份，注意！！！返回的月 1月就是1！！！ 而不是0
	 * <br>跟Calendar类返回月不一样，这点要注意！！！
	 * @param time
	 * @return
	 */
	public static int getMonth(long time){
		Calendar cal = getCal(time);
		return cal.get(Calendar.MONTH) + 1;
	}
	
	public static int getDay(long time){
		Calendar cal = getCal(time);
		return cal.get(Calendar.DAY_OF_MONTH);
	}
	
	public static int getHour(long time){
		Calendar cal = getCal(time);
		return cal.get(Calendar.HOUR_OF_DAY);
	}
	
	public static int getMinute(long time){
		Calendar cal = getCal(time);
		return cal.get(Calendar.MINUTE);
	}
	
	public static int getSecond(long time){
		Calendar cal = getCal(time);
		return cal.get(Calendar.SECOND);
	}
	
	public static int getMillSec(long time){
		Calendar cal = getCal(time);
		return cal.get(Calendar.MILLISECOND);
	}
	
	/**
	 * 取得时间所在季度的季度开始月份
	 * <br>注意！！！这里的返回的月也是以1月为年第一月的
	 * @param time
	 * @return
	 */
	public static int getQuartStartMonth(long time){
		Calendar cal = getCal(time);
		int m = cal.get(Calendar.MONTH);
		return m / 3 * 3 + 1;
	}
	
	/**
	 * 取得指定时间所在季度的开始月份的1日0时0分0秒0豪
	 * @param time
	 * @return
	 */
	public static long getQuartStart(long time){
		Calendar cal = getCal(time);
		cal.set(Calendar.DAY_OF_MONTH, 1);
		cal.set(Calendar.MONTH, getQuartStartMonth(time) - 1);
		cal.set(Calendar.HOUR_OF_DAY, 0);
		cal.set(Calendar.MINUTE, 0);
		cal.set(Calendar.SECOND, 0);
		cal.set(Calendar.MILLISECOND, 0);
		
		return cal.getTimeInMillis();
	}

	/**
	 * 将某个时间 设置为特定的月
	 * <br>注意！！！这里的月也是以1月为年第一月
	 * <br>注意！！如果原日期所在日在目标月没有 那么将默认为目标月最后一日
	 * <br>例如3月31日 设置为2月 将设置为2月28日
	 * @param time
	 * @param month
	 * @return
	 */
	public static long setMonth(long time, int month){
		Calendar cal = getCal(time);
		int srcM = cal.get(Calendar.MONTH) + 1;
		cal.add(Calendar.MONTH, month - srcM);
		return cal.getTimeInMillis();
	}
	
	/**
	 * 设置日 超过月最后日 将变成下月 注意！！！
	 * @param time
	 * @param day
	 * @return
	 */
	public static long setDay(long time, int day){
		Calendar cal = getCal(time);
		cal.set(Calendar.DAY_OF_MONTH, day);
		return cal.getTimeInMillis();
	}
	
	/**
	 * 设置24制小时
	 * @param time
	 * @param hour
	 * @return
	 */
	public static long setHourOfDay(long time, int hour){
		Calendar cal = getCal(time);
		cal.set(Calendar.HOUR_OF_DAY, hour);
		return cal.getTimeInMillis();
	}
	
	/**
	 * 设置分钟
	 * @param time
	 * @param minute
	 * @return
	 */
	public static long setMinute(long time, int minute){
		Calendar cal = getCal(time);
		cal.set(Calendar.MINUTE, minute);
		return cal.getTimeInMillis();
	}
	
	/**
	 * 设置秒
	 * @param time
	 * @param sec
	 * @return
	 */
	public static long setSecond(long time, int sec){
		Calendar cal = getCal(time);
		cal.set(Calendar.SECOND, sec);
		return cal.getTimeInMillis();
	}
	
	/**
	 * 设置毫秒
	 * @param time
	 * @param ms
	 * @return
	 */
	public static long setMillSec(long time, int ms){
		Calendar cal = getCal(time);
		cal.set(Calendar.MILLISECOND, ms);
		return cal.getTimeInMillis();
	}
	
	/**
	 * 设置某个时间 注意！月是以每年一月为1开始的！！！
	 * <br>注意：某个字段如果设为-1，那么意即不修改这个字段！
	 * @param time
	 * @param year
	 * @param month
	 * @param day
	 * @param hourOfDay
	 * @param minute
	 * @param second
	 * @param millsecond
	 * @return
	 */
	public static long setTime(long time, int year, int month, int day, int hourOfDay, int minute
			, int second, int millsecond){
		Calendar cal = getCal(time);
		
		if(millsecond >= 0)
			cal.set(Calendar.MILLISECOND, millsecond);
		if(second >= 0)
			cal.set(Calendar.SECOND, second);
		if(minute >= 0)
			cal.set(Calendar.MINUTE, minute);
		if(hourOfDay >= 0)
			cal.set(Calendar.HOUR_OF_DAY, hourOfDay);
		if(day >= 0)
			cal.set(Calendar.DAY_OF_MONTH, day);
		if(month >= 1)
			cal.set(Calendar.MONTH, month - 1);
		if(year >= 0)
			cal.set(Calendar.YEAR, year);
		
		return cal.getTimeInMillis();
	}


	/**
	 * 根据时间和粒度算出上一个时间段的结束时间
	 * @param time
	 * @param interval
	 * @return
	 */
	public static long getEndTime(long time ,int interval ){
		return time/(interval*60*1000l)*(interval*60*1000l);
	}




	/***
	 *  得到本周开始时间
	 * @param time
	 * @return
	 */
	public static long getWeekDayStartTime(long time){
		Calendar currentDate = new GregorianCalendar();
		currentDate.setFirstDayOfWeek(Calendar.MONDAY);
		currentDate.setTimeInMillis(time);

		currentDate.set(Calendar.HOUR_OF_DAY, 0);
		currentDate.set(Calendar.MINUTE, 0);
		currentDate.set(Calendar.SECOND, 0);
		currentDate.set(Calendar.DAY_OF_WEEK, Calendar.MONDAY);
		return currentDate.getTimeInMillis();
	}

	/***
	 * 得到本周结束时间
	 * @return
	 */


	public static long getWeekDayEndTime(long time){
		Calendar currentDate = new GregorianCalendar();
		currentDate.setTimeInMillis(time);
		currentDate.setFirstDayOfWeek(Calendar.MONDAY);
		currentDate.set(Calendar.HOUR_OF_DAY, 23);
		currentDate.set(Calendar.MINUTE, 59);
		currentDate.set(Calendar.SECOND, 59);
		currentDate.set(Calendar.DAY_OF_WEEK, Calendar.SUNDAY);

		return currentDate.getTimeInMillis();
	}




	/**
	 * 返回指定时间所在月的1日0点0分0秒0豪
	 * @param time
	 * @return
	 */
	public static long getMonthDStart(long time){
		Calendar c = getCal(time);
		c.set(Calendar.DAY_OF_MONTH, 1);
		c.set(Calendar.HOUR_OF_DAY, 0);
		c.set(Calendar.MINUTE, 0);
		c.set(Calendar.SECOND, 0);
		c.set(Calendar.MILLISECOND, 0);

		return c.getTimeInMillis();
	}


	/**
	 * 返回指定时间所在月最后日23点59分59秒59毫秒
	 * @param time
	 * @return
	 */
	public static long getMonthDayEnd(long time){
		Calendar c = getCal(time);
		c.set(Calendar.DAY_OF_MONTH, c.getActualMaximum(Calendar.DATE));
		c.set(Calendar.HOUR_OF_DAY, 23);
		c.set(Calendar.MINUTE, 59);
		c.set(Calendar.SECOND, 59);
		c.set(Calendar.MILLISECOND, 59);

		return c.getTimeInMillis();
	}

	/**
	 * 返回指定时间所在日的0点0分0秒0豪秒
	 * @param time
	 * @return
	 */
	public static long getDayDayStart(long time){
		Calendar c = getCal(time);
		c.set(Calendar.HOUR_OF_DAY, 0);
		c.set(Calendar.MINUTE, 0);
		c.set(Calendar.SECOND, 0);
		c.set(Calendar.MILLISECOND, 0);

		return c.getTimeInMillis();
	}

	/**
	 * 返回指定时间所在日最后的23点59分59秒59毫秒
	 * @param time
	 * @return
	 */
	public static long getDayDayEnd(long time){
		Calendar c = getCal(time);
		c.set(Calendar.HOUR_OF_DAY, 23);
		c.set(Calendar.MINUTE, 59);
		c.set(Calendar.SECOND, 59);
		c.set(Calendar.MILLISECOND, 59);

		return c.getTimeInMillis();
	}


	public static String getTableName(Integer buslogInterval) {
		SimpleDateFormat dayformat = new SimpleDateFormat(
				"yyyyMMdd");
		if(buslogInterval==0){
			return "";
		}else if(buslogInterval==1){

			String time = dayformat.format(new Date());
			return "_"+time;
		}else if(buslogInterval==7){
			long time = new Date().getTime();
			Calendar currentDate = new GregorianCalendar();
			currentDate.setFirstDayOfWeek(Calendar.MONDAY);
			currentDate.setTimeInMillis(time);
			currentDate.set(Calendar.DAY_OF_WEEK, Calendar.MONDAY);
			long startTime = currentDate.getTimeInMillis();
			String startTimes = dayformat.format(startTime);
			currentDate.set(Calendar.DAY_OF_WEEK, Calendar.SUNDAY);
			long endTime = currentDate.getTimeInMillis();
			String endTimes = dayformat.format(endTime);
			return "_"+startTimes+"_"+endTimes;
		}else if(buslogInterval==30){
			long time = new Date().getTime();
			Calendar c = getCal(time);
			c.set(Calendar.DAY_OF_MONTH, 1);
			long startTime = c.getTimeInMillis();
			String startTimes = dayformat.format(startTime);
			c.set(Calendar.DAY_OF_MONTH, c.getActualMaximum(Calendar.DATE));
			long endTime = c.getTimeInMillis();
			String endTimes = dayformat.format(endTime);
			return "_"+startTimes+"_"+endTimes;
		}
		return "";
	}


	public static void main(String args[]) throws Exception{


		System.out.println(getTableName(7, TimeTool.dateMove(new Date().getTime(),-14)));

		//System.out.println(getTableName(30));
		String[] arg = { "20180322"};
		System.out.println(TimeTool.genDbInfo(arg));

		String[] arg0 = new String[]{};
		System.out.println(TimeTool.genDbInfo(arg0));


	}


	public static String getTableName(Integer buslogInterval,long dateTime) {
		SimpleDateFormat dayformat = new SimpleDateFormat(
				"yyyyMMdd");
		if(buslogInterval==0){
			return "";
		}else if(buslogInterval==1){

			String time = dayformat.format(dateTime);
			return "_"+time;
		}else if(buslogInterval==7){
			long time = dateTime;
			Calendar currentDate = new GregorianCalendar();
			currentDate.setFirstDayOfWeek(Calendar.MONDAY);
			currentDate.setTimeInMillis(time);
			currentDate.set(Calendar.DAY_OF_WEEK, Calendar.MONDAY);
			long startTime = currentDate.getTimeInMillis();
			String startTimes = dayformat.format(startTime);
			currentDate.set(Calendar.DAY_OF_WEEK, Calendar.SUNDAY);
			long endTime = currentDate.getTimeInMillis();
			String endTimes = dayformat.format(endTime);
			return "_"+startTimes+"_"+endTimes;
		}else if(buslogInterval==30){
			long time = dateTime;
			Calendar c = getCal(time);
			c.set(Calendar.DAY_OF_MONTH, 1);
			long startTime = c.getTimeInMillis();
			String startTimes = dayformat.format(startTime);
			c.set(Calendar.DAY_OF_MONTH, c.getActualMaximum(Calendar.DATE));
			long endTime = c.getTimeInMillis();
			String endTimes = dayformat.format(endTime);
			return "_"+startTimes+"_"+endTimes;
		}
		return "";
	}


}
