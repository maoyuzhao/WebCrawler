package edu.bupt.util;

import org.jsoup.nodes.Document;

import edu.bupt.crawler.main.Crawler;

import java.util.ArrayList;
import java.util.StringTokenizer;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Created with IntelliJ IDEA. User: guohuawu Date: 11/7/13
 */
public class JudgeContent {

	public static void main(String[] args) throws Exception {
		Crawler crawler = new Crawler();
		Crawler.Downloader downloader = crawler.new Downloader();
		Document doc = downloader.downloadHtml("http://news.sina.com.cn/c/2013-11-08/034728651752.shtml");
		long start = System.currentTimeMillis();
		boolean flag = isDateLegal(doc, "2013-11-09" ,"2020-01-01", "\\d{4}年\\d{2}月\\d{2}日");
		long end = System.currentTimeMillis();
		System.out.println(flag + " " + (end - start));
	}

	private static JudgeContent instance = new JudgeContent();

	private JudgeContent() {

	}

	public static boolean isDateLegal(Document doc, String startDateString, String endDateString, String datePattern) {
		String html = doc.body().text();
		boolean flag = false;
		flag = isDateLegal(html, startDateString, endDateString, datePattern);
		return flag;
	}

	/**
	 * 判断该页面是否已过期，判断依据是抽取文中的时间信息 （以最接近现在的时间为准），过期时间为deadLine（格式自己决定），
	 * 抽取规则由timePattern给出，如果需要额外信息可以另外补充。
	 * 
	 * @param html
	 *            待处理页面
	 * @param deadLine
	 *            截止日期，时间格式：2013-11-11
	 * @param datePatternString
	 *            时间抽取模板
	 * @return
	 */
	public static boolean isDateLegal(String html, String startDateString, String endDateString, String datePatternString) {
		Matcher dateMatcher = null;
		dateMatcher = Pattern.compile("(\\d+)-(\\d+)-(\\d+)").matcher(startDateString); // deadline时间的格式2013-11-11
		MyDate startDate = null; // 把deadLine存储成可比较的MyDate
		if (dateMatcher.matches()) {
			startDate = instance.new MyDate();
			startDate.y = Integer.parseInt(dateMatcher.group(1));
			startDate.m = Integer.parseInt(dateMatcher.group(2));
			startDate.d = Integer.parseInt(dateMatcher.group(3));
		}
		dateMatcher = Pattern.compile("(\\d+)-(\\d+)-(\\d+)").matcher(endDateString); // deadline时间的格式2013-11-11
		MyDate endDate = null; // 把deadLine存储成可比较的MyDate
		if (dateMatcher.matches()) {
			endDate = instance.new MyDate();
			endDate.y = Integer.parseInt(dateMatcher.group(1));
			endDate.m = Integer.parseInt(dateMatcher.group(2));
			endDate.d = Integer.parseInt(dateMatcher.group(3));
		}
		// 从网页中找出所有的日期
		// 比较之后保存一个最新的
		// 用最新的日期和deadlineDate对比
		Pattern datePattern = Pattern.compile(datePatternString); // 获取时间的正则表达式
		Matcher matcher = datePattern.matcher(html);
		ArrayList<String> dates = new ArrayList<>();
		while (matcher.find()) {
			dates.add(matcher.group(0));
		}
		MyDate date = instance.findLatestDate(dates);
		if (date == null || !instance.isInDatePeriod(startDate, endDate, date)) {
			return false;
		} else {
			return true;
		}
	}

	/**
	 * 存储日期，包括年、月、日
	 */
	private class MyDate {
		public MyDate() {
			y = 0;
			m = 0;
			d = 0;
		}

		int y;
		int m;
		int d;
	}

	/**
	 * 返回对应月份的天数
	 * 
	 * @param year
	 *            年份
	 * @param month
	 *            月份
	 * @return month月份的天数
	 */
	private int NumDaysOfMonth(int year, int month) {
		switch (month) {
		case 1:
		case 3:
		case 5:
		case 7:
		case 8:
		case 10:
		case 12:
			return 31;
		case 4:
		case 6:
		case 9:
		case 11:
			return 30;
		case 2:
			if ((year % 100 != 0 && year % 4 == 0) || (year % 400 == 0)) {
				return 29;
			} else
				return 28;
		default:
			return 0;
		}
	}

	/**
	 * 判断给定的年月日是否是一个合法的日期
	 * 
	 * @param year
	 *            年份
	 * @param month
	 *            月份
	 * @param day
	 *            天
	 * @return 是否合法
	 */
	private boolean isDate(int year, int month, int day) {
		if (year < 0 || month < 1 || month > 12 || day < 1 || day > NumDaysOfMonth(year, month)) {
			return false;
		}
		return true;
	}

	/**
	 * 判断给定的MyDate对象里面的年月日是否合法
	 * 
	 * @param date
	 *            日期
	 * @return 是否合法
	 */
	private boolean isDate(MyDate date) {
		return isDate(date.y, date.m, date.d);
	}

	/**
	 * 比较一系列日期，找出距离现在最近的那个
	 * 
	 * @param dates
	 *            日期列表
	 * @return 最新的日期
	 */
	MyDate findLatestDate(ArrayList<String> dates) {
		MyDate latestDate = new MyDate();
		MyDate tempDate;
		if (dates == null || dates.isEmpty()) {
			return null;
		}
		for (String date : dates) {
			if ((tempDate = parseDate(date)) != null) {
				if (isDate(tempDate)) {
					if (laterDate(latestDate, tempDate)) {
						latestDate = tempDate;
					}
				}
			}
		}
		if (isDate(latestDate)) {
			return latestDate;
		}
		return null;
	}

	/**
	 * 把一个字符串日期转换成一个MyDate日期
	 * 
	 * @param date
	 *            字符串日期
	 * @return MyDate日期
	 */
	private MyDate parseDate(String date) {
		MyDate d = new MyDate();
		ArrayList<String> digits = parseDateToStringList(date);
		if (digits.size() == 3) {
			if (digits.get(0).length() == 2) {
				if (digits.get(0).charAt(0) == '0' || digits.get(0).charAt(0) == '1') {
					d.y = Integer.parseInt("20" + digits.get(0));
				} else {
					d.y = Integer.parseInt("19" + digits.get(0));
				}
			} else if (digits.get(0).length() == 4) {
				if (digits.get(0).startsWith("19") || digits.get(0).startsWith("20")) {
					d.y = Integer.parseInt(digits.get(0));
				} else {
					return null;
				}
			} else {
				return null;
			}
			d.m = Integer.parseInt(digits.get(1));
			d.d = Integer.parseInt(digits.get(2));
			if (!isDate(d)) {
				return null;
			}
		} else {
			return null;
		}
		return d;
	}

	/**
	 * 把日期字符串解析成一个年月日列表 例如：2012-12-12会被解析成2012 12 12
	 * 
	 * @param date
	 *            字符串形式的日期
	 * @return 年月日数组
	 */
	private ArrayList<String> parseDateToStringList(String date) {
		ArrayList<String> digits = new ArrayList<String>();
		StringTokenizer stringTokenizer = new StringTokenizer(date, "./-年月日", false);
		while (stringTokenizer.hasMoreTokens()) {
			digits.add(stringTokenizer.nextToken());
		}
		return digits;
	}

	/**
	 * 判断两个日期哪个离现在更近一些 即date2是否比date1要新就返回true 否则返回false
	 * 
	 * @param date1
	 *            MyDate日期
	 * @param date2
	 *            MyDate日期
	 * @return date1和date2的比较结果
	 */
	private boolean laterDate(MyDate date1, MyDate date2) {
		if (date1 == null || date2 == null) {
			return false;
		}
		if (date1.y < date2.y || date1.y == date2.y && date1.m < date2.m || date1.y == date2.y && date1.m == date2.m && date1.d < date2.d) {
			return true;
		}
		return false;
	}
	

	private boolean isInDatePeriod(MyDate start, MyDate end, MyDate date) {
		if (start == null || end == null || date == null) {
			return false;
		}
		if (start.y < date.y && end.y > date.y) {
			return true;
		} else if (start.y == date.y && (start.m < date.m || start.m == date.m && start.d <= date.d)) {
			return true;
		} else if (end.y == date.y && (end.m > date.m || end.m == date.m && end.d >= date.d)) {
			return true;
		}
		return false;
	}
}
