package edu.bupt.util;

import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class RegexManager {
	
	public static void main(String[] args) {
		String input = "http://techcrunch.cn/2014/05/05/snapchat-adds-text-chat-and-video-calls";
		String regex = "http://techcrunch.cn/\\d{4}/\\d{2}/\\d{2}/([^/]+)/?";
		System.out.println(group(regex, input, 1));
	}
	private RegexManager(){
		
	}
	
	public static boolean find(String regex, String input){
		return Pattern.compile(regex).matcher(input).find();
	}
	
	public static String group(String regex, String input, int group){
		Matcher matcher = Pattern.compile(regex).matcher(input);
		String res = null;
		if (matcher.find()) {
			res = matcher.group(group);
		}
		return res;
	}
}
