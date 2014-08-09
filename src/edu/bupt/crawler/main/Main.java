package edu.bupt.crawler.main;

public class Main {
	public static void main(String[] args) {
		if (args == null || args.length == 0)
		{
			String errInfoString = 	"No parameter! You should use one of the parameters below!\n" +
									"crawl    -for running crawler\n" +
									"parse    -for running parser\n" +
									"filter   -for running filter\n" +
									"separate -for running separator\n";
			System.err.println(errInfoString);
		}
		String cmd = args[0];
		switch (cmd) {
		case "crawl":
			Crawler crawler = new Crawler();
			crawler.beginCrawl();
			break;
		case "parse":
			Parser parser = new Parser();
			parser.beginParse();
			break;
		case "filter":
			Filter filter = new Filter();
			filter.beginFilter();
			break;
		case "separate":
			Separator separator = new Separator();
			separator.beginSeparator();
			break;
		default:
			System.err.println("args error please try again!"+
									"You should use one of the parameters below!\n" +
									"crawl    -for running crawler\n" +
									"parse    -for running parser\n" +
									"filter   -for running filter\n" +
									"separate -for running separator\n");
			break;
		}
	}
}
