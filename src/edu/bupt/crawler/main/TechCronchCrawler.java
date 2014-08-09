package edu.bupt.crawler.main;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.InputStreamReader;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;
import java.util.Date;
import java.util.LinkedHashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

import org.apache.commons.configuration.XMLConfiguration;
import org.apache.commons.configuration.reloading.FileChangedReloadingStrategy;
import org.apache.commons.io.FileUtils;
import org.apache.log4j.Logger;
import org.apache.log4j.PropertyConfigurator;
import org.jsoup.Jsoup;
import org.jsoup.Connection.Response;
import org.jsoup.nodes.Document;
import org.jsoup.nodes.Element;
import org.jsoup.select.Elements;

import de.l3s.boilerpipe.extractors.ArticleExtractor;
import edu.bupt.util.BloomFilter;
import edu.bupt.util.JudgeContent;
import edu.bupt.util.KVPair;
import edu.bupt.util.RegexManager;

public class TechCronchCrawler {
	private static final int SINGLE = 0;
	private static final int MULTIPLE = 1;
	private final Logger logger = Logger.getLogger(TechCronchCrawler.class);

	private XMLConfiguration config;
	private byte[] Locker;
	private byte[] pFileLocker;
	private byte[] upFileLocker;

	private BloomFilter<String> bfUrl; // BloomFilter用于URL去重
	private LinkedList<File> urlFileQueue; // 待爬队列文件队列，会随爬虫运行先变大后变小
	private LinkedBlockingQueue<String> urlQueue; // 当前待爬队列，只会减少
	private LinkedBlockingQueue<String> urlCache; // 待爬队列文件队列的缓存队列，满了写入文件，并加入待爬队列文件队列
	private LinkedBlockingQueue<KVPair<String, Document>> docQueue; // 已下载页面文档队列，KVPairs中key为URL，value为页面文档
	private LinkedBlockingQueue<String> pVisitedCache; // 已抽正文已访问队列
	private LinkedBlockingQueue<String> upVisitedCache; // 未抽正文已访问队列
	private StringBuffer textBuffer; // 文本文件队列

	private File pVisitedFile = new File("URL" + File.separator + "pVisited.txt");
	private File upVisitedFile = new File("URL" + File.separator + "upVisited.txt");
	private File currentUrlFile = new File("URL" + File.separator + "currentUrl.txt");
	private File urlCacheFile = new File("URL" + File.separator + "urlCache.txt");
	private File rejectedUrlFile = new File("URL" + File.separator + "rejectedUrl.txt");
	private File urlPoolDir = new File("URL" + File.separator + "urlPool");

	private int maxBodySize; // 页面最大阈值，单位为byte，默认为1MB
	private int timeout; // 连接超时，0为无超时，默认为0
	private int sleepTime; // downloader线程每次下载完后的休眠时间，默认为4s
	private int dThreadNum; // downloader线程数，默认为30个
	private int pThreadNum; // parser线程数，默认为3个
	private int storageType; // 存储格式，分为mutiple(1)和single(0)，默认为single(0)
	private int maxUrlQueueSize; // urlQueue最大长度，必须大于2000，默认为200000
	private int maxDocQueueSize; // docQueue最大程度，0为无限制，默认为300
	private int htmlNumOfEachTimestamp; // 每个timestamp中包含的html页面数，默认为20000
	private boolean useProxy; // 是否使用代理，默认为false
	private boolean useParse; // 是否在下载的同时抽取正文，默认为true
	private boolean isStoreParsedHtml; // 是否保存parsed html，当且仅当usePares为true时有效
	private boolean useDateFilter; // 是否使用时间过滤，过滤过期的网页
	private String startDate; // 如果使用时间过滤，则接收时间的开始日期
	private String endDate; // 如果使用时间过滤，则接收时间的结束日期
	private String encoding; // 设置下载页面编码格式，默认为
	private String timestamp; // 时间戳
	private String storePath; // 存储路径，默认为当前路径下的Website文件夹中
	private String rjRegex; // 拒绝访问的url pattern
	private String acRegex; // 允许访问的url pattern
	private String dateRegex; // 从文中抽时间的正则表达式
	private List<Object> acSelector; // 正文部分的css selector，当useparse为true时必须设置
	private List<Object> rjSelector; // 需要剔除部分的css selector
	private LinkedList<KVPair<String, String>> urlReplacement; // 需要替换的url字段

	public static void main(String[] args) {
		TechCronchCrawler crawler = new TechCronchCrawler();
		crawler.beginCrawl();
	}

	public void beginCrawl() {
		/******************************** 配置文件初始化 ********************************/
		PropertyConfigurator.configure("log4j.properties");
		try {
			/**
			 * 设置配置文件重载策略，每10秒钟检测一次
			 */
			config = new XMLConfiguration("crawler.xml");
			FileChangedReloadingStrategy strategy = new FileChangedReloadingStrategy();
			strategy.setRefreshDelay(10000);
			config.setReloadingStrategy(strategy);
		} catch (Exception e) {
			logger.warn(e.getMessage(), e);
		}
		/********************************* 参数初始化 *********************************/
		System.out.println("loading \"config\" file ...");
		Locker = new byte[1];
		pFileLocker = new byte[1];
		upFileLocker = new byte[1];
		timeout = config.getInt("crawler.timeout.value", 60000);
		sleepTime = config.getInt("crawler.sleepTime.value", 5000);
		dThreadNum = config.getInt("crawler.downloadThreadNum.value", 30);
		if (dThreadNum <= 0) {
			System.err.println("please set \"downloadThreadNum\" larger than 0");
			System.exit(1);
		}
		pThreadNum = config.getInt("crawler.parseThreadNum.value", 3);
		if (pThreadNum <= 0) {
			System.err.println("please set \"parseThreadNum\" larger than 0");
			System.exit(1);
		}
		htmlNumOfEachTimestamp = config.getInt("crawler.htmlNumOfEachTimestamp.value", 100000);
		encoding = config.getString("crawler.encoding.value", "utf-8");
		timestamp = new SimpleDateFormat("yyyyMMddHHmmss").format(new Date());
		storePath = config.getString("crawler.storePath.value", "Website");
		useProxy = config.getBoolean("crawler.proxy.useProxy.value", false);
		if (useProxy) {
			System.setProperty("http.proxyHost", config.getString("crawler.proxy.host.value", "127.0.0.1"));
			System.setProperty("http.proxyPort", config.getString("crawler.proxy.port.value", "8087"));
		}
		acRegex = setRegex("crawler.acRegex.value");
		rjRegex = setRegex("crawler.rjRegex.value");
		useDateFilter = config.getBoolean("crawler.dateFilter.useDateFilter", false);
		if (useDateFilter) {
			dateRegex = setRegex("crawler.dateFilter.dateRegex.value");
			if (dateRegex == null) {
				System.err
						.println("You choose \"useDateFilter\", but haven't set the \"crawler.dateFilter.dateRegex.value\", please check and try later");
			}
			startDate = config.getString("crawler.dateFilter.startDate.value");
			endDate = config.getString("crawler.dateFilter.endDate.value");
			if (!startDate.matches("\\d{4}-\\d{2}-\\d{2}") || !endDate.matches("\\d{4}-\\d{2}-\\d{2}") || startDate.compareTo(endDate) > 0) {
				System.err
						.println("\"startDate\" or \"endDate\" do not match the Pattern \"\\d{4}-\\d{2}-\\d{2}\", or startDate is later than endDate. Please check and try later.");
				System.exit(1);
			}
		}

		String storageType = config.getString("crawler.storageType.value", "single");
		if (storageType.equalsIgnoreCase("single")) {
			this.storageType = TechCronchCrawler.SINGLE;
		} else {
			this.storageType = TechCronchCrawler.MULTIPLE;
		}

		useParse = config.getBoolean("crawler.parse.useParse", false);
		if (!useParse) {
			isStoreParsedHtml = true;
		} else {
			isStoreParsedHtml = config.getBoolean("crawler.parse.isStore", true);
			textBuffer = new StringBuffer();
			pVisitedCache = new LinkedBlockingQueue<>();
			acSelector = config.getList("crawler.parse.acSelector.value", new ArrayList<Object>());
			if (acSelector.isEmpty()) {
				System.err.println("You choose \"useParse\", but haven't set the \"crawler.parse.acSelector.value\", please check and try later");
				System.exit(1);
			}
			rjSelector = config.getList("crawler.parse.rjSelector.value", new ArrayList<Object>());
		}
		upVisitedCache = new LinkedBlockingQueue<>();
		double errorRate = config.getDouble("crawler.bloomFilter.errorRate.value", 0.0001);
		int expectedNumberOfElements = config.getInt("crawler.bloomFilter.expectedNumberOfElements.value", 160000000);
		bfUrl = new BloomFilter<>(errorRate, expectedNumberOfElements);
		maxBodySize = config.getInt("crawler.maxBodySize.value", 1024 * 1024);
		maxUrlQueueSize = config.getInt("crawler.maxUrlQueueSize.value", 200000);
		maxDocQueueSize = config.getInt("crawler.maxDocQueueSize.value", 300);
		if (maxDocQueueSize == 0) {
			docQueue = new LinkedBlockingQueue<>();
		} else {
			docQueue = new LinkedBlockingQueue<>(maxDocQueueSize);
		}
		if (maxUrlQueueSize < 2000) {
			System.err.println("==========>maxUrlQueueSize has automatically set 2000<==========");
			maxUrlQueueSize = 2000;
		}
		urlFileQueue = new LinkedList<>();
		urlQueue = new LinkedBlockingQueue<>();
		urlCache = new LinkedBlockingQueue<>();
		urlReplacement = setUrlReplacement();
		/********************************* 参数初始化 *********************************/

		/*
		 * 程序开始
		 */
		try {
			createFiles();
			initContainer();
			startThread();
		} catch (Exception e) {
			logger.warn(e.getMessage(), e);
		}
	}

	private void createFiles() throws Exception {
		File storePathFile = new File(storePath);
		if (!storePathFile.exists()) {
			storePathFile.mkdirs();
		}
		if (useParse && !pVisitedFile.exists()) {
			pVisitedFile.createNewFile();
		}
		if (!upVisitedFile.exists()) {
			upVisitedFile.createNewFile();
		}
		if (!currentUrlFile.exists()) {
			currentUrlFile.createNewFile();
		}
		if (!urlCacheFile.exists()) {
			urlCacheFile.createNewFile();
		}
		if (!rejectedUrlFile.exists()) {
			rejectedUrlFile.createNewFile();
		}
		if (!urlPoolDir.exists()) {
			urlPoolDir.mkdirs();
		}
	}

	/**
	 * 初始化所有容器
	 * 
	 * @throws Exception
	 */
	private void initContainer() throws Exception {
		/************* initial urlQueue ************/
		System.out.println("loading \"currentUrlFile\" ...");
		urlQueue.addAll(FileUtils.readLines(currentUrlFile, "utf-8"));
		if (urlQueue.isEmpty()) {
			String seedUrl = config.getString("crawler.seed.value");
			Collection<String> list = new Parser().extract(new Downloader().downloadHtml(seedUrl), acRegex, rjRegex);
			urlQueue.addAll(list);
			urlQueue.add(seedUrl);
		}

		/************* initial urlCache ************/
		System.out.println("loading \"urlCacheFile\" ...");
		urlCache.addAll(FileUtils.readLines(urlCacheFile, "utf-8"));

		/************* initial bfURL ***************/
		System.out.println("loading \"bfURL\" ...");
		bfUrl.addAll(urlQueue);
		bfUrl.addAll(urlCache);
		if (useParse) {
			addtoBF(pVisitedFile);
		}
		addtoBF(upVisitedFile);

		/************* initial urlFileQueue ********/
		System.out.println("loading \"urlFileQueue\" ...");
		File[] urlFiles = urlPoolDir.listFiles();
		for (File urlfile : urlFiles) {
			addtoBF(urlfile);
			urlFileQueue.add(urlfile);
		}
		Collections.sort(urlFileQueue, new Comparator<File>() {
			@Override
			public int compare(File f1, File f2) {
				return f1.getName().compareTo(f2.getName());
			}
		});
	}

	/**
	 * 将文件中的url加入到布朗过滤器
	 * 
	 * @param 文件名
	 * @throws Exception
	 */
	private void addtoBF(File filename) throws Exception {
		BufferedReader br = null;
		br = new BufferedReader(new InputStreamReader(new FileInputStream(filename), "utf-8"));
		String line = null;
		while ((line = br.readLine()) != null) {
			bfUrl.add(line);
		}
		br.close();
	}

	private LinkedList<KVPair<String, String>> setUrlReplacement() {
		List<Object> source = config.getList("crawler.urlReplacement.source", new ArrayList<>());
		List<Object> target = config.getList("crawler.urlReplacement.target", new ArrayList<>());
		LinkedList<KVPair<String, String>> urlReplacement = new LinkedList<>();
		if (source.size() != target.size()) {
			System.err.println("urlReplacement set error, please check it");
		} else {
			for (int i = 0; i < source.size(); i++) {
				urlReplacement.add(new KVPair<>(source.get(i).toString(), target.get(i).toString()));
			}
		}
		return urlReplacement;
	}

	/**
	 * 拼接正则表达式
	 * 
	 * @param config文件中的键值
	 * @return
	 */
	private String setRegex(String key) {
		List<Object> regexes = config.getList(key, new ArrayList<>());
		String result = null;
		for (Object regex : regexes) {
			if (result == null) {
				result = regex.toString();
			} else {
				result += "|" + regex.toString();
			}
		}
		return result;
	}

	/**
	 * 开启所有线程
	 * 
	 * @throws Exception
	 */
	private void startThread() throws Exception {
		ScheduledExecutorService scheExec = Executors.newScheduledThreadPool(1);
		ExecutorService dExec = Executors.newFixedThreadPool(dThreadNum);
		ExecutorService pExec = Executors.newFixedThreadPool(pThreadNum);
		scheExec.scheduleAtFixedRate(new Loger(pExec), 10, 10, TimeUnit.SECONDS);

		dExec.execute(new Downloader());
		pExec.execute(new Parser(dExec));
		while (--dThreadNum > 0 && --pThreadNum > 0) {
			dExec.execute(new Downloader());
			pExec.execute(new Parser(dExec));
			Thread.sleep(1000);
		}
		while (dThreadNum-- > 0) {
			dExec.execute(new Downloader());
			Thread.sleep(1000);
		}
		while (pThreadNum-- > 0) {
			pExec.execute(new Parser(dExec));
			Thread.sleep(1000);
		}
		dExec.shutdown();
		pExec.shutdown();
	}

	public class Downloader implements Runnable {

		/**
		 * 下载url对应的页面
		 * 
		 * @param url
		 * @return 页面的Document对象
		 * @throws Exception
		 */
		public Document downloadHtml(String url) throws Exception {
			Document doc = null;
			Response rep = Jsoup.connect(url)
					.userAgent("Mozilla/5.0 (Windows NT 6.1; WOW64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/28.0.1500.72 Safari/537.36")
					.ignoreHttpErrors(true).referrer(url).maxBodySize(maxBodySize).timeout(timeout).execute();
			int statusCode = rep.statusCode();
			if (statusCode == 200) {
				doc = rep.parse();
			} else {
				logger.error(url);
				System.err.printf("%-16s %-10s\t%s\n", Thread.currentThread().getName(), statusCode, url);
			}
			return doc;
		}

		/**
		 * 获取符合条件的URL
		 * 
		 * @return 符合条件的URL
		 * @throws InterruptedException
		 */
		private String getUrl() throws Exception {
			String url = null;
			List<String> rjUrl = new ArrayList<>();
			while (true) {
				url = urlQueue.poll(15, TimeUnit.SECONDS);
				if (url == null) {
					if (urlQueue.isEmpty() && urlCache.isEmpty() && urlFileQueue.isEmpty()) {
						break;
					} else {
						continue;
					}
				} else if ((rjRegex != null && RegexManager.find(rjRegex, url)) || (acRegex != null && !RegexManager.find(acRegex, url))) {
					rjUrl.add(url);
					if (rjUrl.size() % 50000 == 0) {
						synchronized (Locker) {
							FileUtils.writeLines(rejectedUrlFile, "utf-8", rjUrl, "\n", true);
						}
						rjUrl.clear();
					}
				} else if (acRegex == null || RegexManager.find(acRegex, url)) {
					if (!urlReplacement.isEmpty()) {
						for (KVPair<String, String> pair : urlReplacement) {
							url = url.replaceAll(pair.getKey(), pair.getValue());
						}
					}
					break;
				}
			}
			if (!rjUrl.isEmpty()) {
				synchronized (Locker) {
					FileUtils.writeLines(rejectedUrlFile, "utf-8", rjUrl, "\n", true);
				}
			}
			return url;
		}

		@Override
		public void run() {
			String url = null;
			Document doc = null;
			KVPair<String, Document> paire = null;
			while (true) {
				try {
					url = getUrl(); // 获取URL连接
					if (url == null) {
						break;
					}
					doc = downloadHtml(url); // 通过URL下载页面
					if (doc != null) {
						paire = new KVPair<>(url, doc);
						while (!docQueue.offer(paire)) { // 将下载页面入队，如果不成功（队列已满）则等待1s后重试，直至添加成功
							System.err.println("add to docQueue failed, 1s later to try again: " + url);
							Thread.sleep(1000);
						}
						Thread.sleep(sleepTime);
					}
				} catch (Exception e) {
					logger.error(url);
					logger.warn(e.getMessage(), e);
				}
			}
			logger.info(Thread.currentThread().getName() + "(downloader) is finished");
		}

	}

	public class Parser implements Runnable {

		private ExecutorService dExec; // downloader线程池

		public Parser(ExecutorService dExec) {
			this.dExec = dExec;
		}

		public Parser() {
		}

		/**
		 * 提取页面的URL
		 * 
		 * @param html
		 *            待提取页面
		 * @param acRegex
		 *            null表示不用接收规则
		 * @param rjRegex
		 *            null表示不用过滤规则
		 * @return 去重后的URLlist
		 */
		public Collection<String> extract(String html, String acRegex, String rjRegex) {
			Document doc = Jsoup.parse(html);
			return extract(doc, acRegex, rjRegex);
		}

		/**
		 * 提取页面DOC的URL
		 * 
		 * @param html
		 *            待提取页面DOC
		 * @param acRegex
		 *            null表示不用接收规则
		 * @param rjRegex
		 *            null表示不用过滤规则
		 * @return 去重后的URLlist
		 */
		public Collection<String> extract(Document doc, String acRegex, String rjRegex) {
			Collection<String> urlList = new LinkedHashSet<>();
			Elements links = doc.select("a[href]");
			String url = null;
			for (Element link : links) {
				url = link.attr("abs:href");
				if (url == null || (rjRegex != null && RegexManager.find(rjRegex, url))) {
					continue;
				} else if (acRegex == null || RegexManager.find(acRegex, url)) {
					urlList.add(url);
				}
			}
			return urlList;
		}

		/**
		 * 提取正文，parse会对doc对象修改，使用时要注意
		 * 
		 * @param doc
		 *            待提取页面DOC
		 * @param acSelector
		 *            接收模块规则
		 * @param rjSelector
		 *            拒绝模块规则
		 * @return
		 */
		// public String jsoupParser(String html, List<?> acSelector, List<?>
		// rjSelector) {
		// Document doc = Jsoup.parse(html);
		// return jsoupParser(doc, acSelector, rjSelector);
		// }

		public String jsoupParser(Document doc, List<?> acSelector, List<?> rjSelector) {
			StringBuilder parsedContent = new StringBuilder();
			boolean flag = false;
			String temp = null;
			for (Object rs : rjSelector) {
				doc.select(rs.toString()).remove();
			}
			doc.select("br").append("BR2N");
			doc.select("p").append("BR2N");
			for (Object as : acSelector) {
				Elements elements = doc.select(as.toString());
				if (elements.isEmpty()) {
					continue;
				} else {
					if (!flag) {
						flag = true;
					}
					for (Element element : elements) {
						temp = element.text().replaceAll("BR2N", "\n").trim();
						if (temp.length() > 0) {
							parsedContent.append(temp + "\n");
						}
					}
				}
			}
			if (flag) {
				return parsedContent.toString();
			} else {
				return null;
			}
		}

		public String boilerpipeParser(String html) throws Exception {
			String text = ArticleExtractor.INSTANCE.getText(html);
			return text;
		}

		/**
		 * Parse正文，并打上相关日志
		 * 
		 * @param doc
		 *            待Parse文档
		 * @param url
		 *            文档URL
		 * @return
		 * @throws Exception
		 */
		private boolean parse(String htmlZH, String htmlEN, Document docZH, Document docEN, String urlZH, String urlEN) throws Exception {
			String contentZH = null;
			String contentEN = null;
			String content = null;
			boolean isParsed = false;
			if (useParse) {
				contentZH = jsoupParser(docZH, acSelector, rjSelector);
				if (docEN != null) {
					contentEN = jsoupParser(docEN, acSelector, rjSelector);
				}
				if (contentZH != null && contentEN != null) {
					isParsed = true;
					content = "<document>\n<zh url=" + urlZH + ">\n" + contentZH + "\n</zh>\n<en url=" + urlEN + ">\n" + contentEN + "\n</EN>\n</document>\n";
					textBuffer.append(content);
					pVisitedCache.add(urlZH);
					System.out.printf("%-16s %-10s\t%s\n", Thread.currentThread().getName(), "\\parse\\", urlZH);
				} else {
					isParsed = false;
					upVisitedCache.add(urlZH);
					System.err.printf("%-16s %-10s\t%s\n", Thread.currentThread().getName(), "/unparse/", urlZH);
				}
			} else {
				upVisitedCache.add(urlZH);
				System.out.printf("%-16s %-10s\t%s\n", Thread.currentThread().getName(), "/fetch/", urlZH);
			}
			return isParsed;
		}

		/**
		 * URL去重及URL处理
		 * 
		 * @param links
		 *            待去重的链接
		 * @return
		 */
		private void filterUrl(Collection<String> links) {
			synchronized (bfUrl) {
				for (String link : links) {
					if (!bfUrl.contains(link)) {
						urlCache.add(link);
						bfUrl.add(link);
					}
				}
			}
		}

		/**
		 * 获取合法的Document-url Paris
		 * 
		 * @return
		 * @throws Exception
		 */
		private KVPair<String, Document> getDocument() throws Exception {
			KVPair<String, Document> pairs = null;
			while (true) {
				pairs = docQueue.poll(2, TimeUnit.SECONDS);
				if (pairs == null) {
					if (dExec.isTerminated()) {
						break;
					} else {
						Thread.sleep(2000);
						continue;
					}
				} else {
					break;
				}
			}
			return pairs;
		}

		private void storeSingle(String storePath, String url, final String html, boolean isParsed) throws Exception {
			String content = html.length() + "\n" + url + "\n" + html;
			File storFile = null;
			if (isParsed) {
				storFile = new File(storePath, "p" + File.separator + timestamp);
				synchronized (pFileLocker) {
					FileUtils.write(storFile, content, encoding, true);
				}
			} else {
				storFile = new File(storePath, "up" + File.separator + timestamp);
				synchronized (upFileLocker) {
					FileUtils.write(storFile, content, encoding, true);
				}
			}
		}

		private void storeMultiple(String storePath, String url, final String html, boolean isParsed) throws Exception {
			String content = html.length() + "\n" + url + "\n" + html;
			String fileName = url.replaceFirst("https?://", "").replaceAll("[.:/\\?\\*|\"<>\\\\]", "");
			File storeFile = null;
			if (isParsed) {
				storeFile = new File(storePath, "p" + File.separator + timestamp + File.separator + fileName);
			} else {
				storeFile = new File(storePath, "up" + File.separator + timestamp + File.separator + fileName);
			}
			String absPath = storeFile.getAbsolutePath();
			if (absPath.length() > 250) {
				absPath = absPath.substring(0, 250);
				storeFile = new File(absPath);
			}
			FileUtils.write(storeFile, content, encoding, false);
		}

		private void store(String url, final String htmlZH, boolean isParsed) throws Exception {
			switch (storageType) {
			case TechCronchCrawler.SINGLE:
				if (isStoreParsedHtml || (!isStoreParsedHtml && !isParsed)) {
					storeSingle(storePath, url, htmlZH, isParsed);
				}
				break;
			case TechCronchCrawler.MULTIPLE:
				if (isStoreParsedHtml || (!isStoreParsedHtml && !isParsed)) {
					storeMultiple(storePath, url, htmlZH, isParsed);
				}
				break;
			default:
				break;
			}
		}

		@Override
		public void run() {
			KVPair<String, Document> pair = null;
			String urlZH = null;
			String urlEN = null;
			String title = null;
			Document docZH = null;
			Document docEN = null;
			String htmlZH = null;
			String htmlEN = null;
			Downloader downloader = new Downloader();
			String regex = "http://techcrunch.cn/\\d{4}/\\d{2}/\\d{2}/([^/]+)";
			boolean isParsed = false;
			while (true) {
				try {
					pair = getDocument();
					if (pair == null) {
						break;
					}
					urlZH = pair.getKey();
					if ((title = RegexManager.group(regex, urlZH, 1)) != null) {
						urlEN = "http://techcrunch.com/" + title;
						docEN = downloader.downloadHtml(urlEN);
						if (docEN != null) {
							htmlEN = docEN.html() + "\n";
						}
					}
					docZH = pair.getValue();
					htmlZH = docZH.html() + "\n";
					
					Collection<String> links = extract(docZH, acRegex, rjRegex);
					filterUrl(links);
					if (useDateFilter && !JudgeContent.isDateLegal(htmlZH, startDate, endDate, dateRegex)) {
						FileUtils.writeStringToFile(rejectedUrlFile, urlZH, "utf-8", true);
						continue;
					}
					isParsed = parse(htmlZH, htmlEN, docZH, docEN, urlZH, urlEN);
					store(urlZH, htmlZH, isParsed);
				} catch (Exception e) {
					logger.warn(e.getMessage(), e);
				}
			}
			logger.info(Thread.currentThread().getName() + "(parser) is finished");
		}
	}

	private class Loger implements Runnable {
		private ExecutorService pExec; // Parser线程池
		private int counter; // 计数器，当计数器到达htmlNumOfEachTimestamp时，更新timestamp
		private int speed; // 当前下载的速度，10秒内的访问量

		public Loger(ExecutorService pExec) {
			this.pExec = pExec;
			counter = 0;
			speed = 0;
		}

		@Override
		public void run() {
			try {
				System.err.println("< === begin log, don't interrupt untile <LOG INFO> is printed ===>");
				updateProperties();
				String info = setLogInfo();
				urlQueueLog();
				visitedCacheLog();
				updateRegex();
				logger.info(info);
				if (!isDiskAvailable()) {
					System.err.println("disk is full!!!");
					System.exit(1);
				}
				if (pExec.isTerminated()) {
					System.exit(0);
				}
			} catch (Exception e) {
				logger.warn(e.getMessage(), e);
			}
		}

		/**
		 * 当磁盘空间不足500MB的时候退出程序
		 * 
		 * @return
		 */
		private boolean isDiskAvailable() {
			boolean flag = true;
			File file = new File(storePath);
			long usableSpace = file.getUsableSpace();
			if (usableSpace < 536870912) {
				flag = false;
			}
			return flag;
		}

		/**
		 * 更新一些变量
		 * 
		 * @throws Exception
		 */
		private void updateProperties() throws Exception {
			htmlNumOfEachTimestamp = config.getInt("crawler.htmlNumOfEachTimestamp.value", 10000);
			if (useParse) {
				counter += (pVisitedCache.size() + upVisitedCache.size());
				speed = (pVisitedCache.size() + upVisitedCache.size()) * 6;
				storeText();
			} else {
				counter += upVisitedCache.size();
				speed = upVisitedCache.size() * 6;
			}
			if (counter >= htmlNumOfEachTimestamp) {
				timestamp = new SimpleDateFormat("yyyyMMddHHmmss").format(new Date());
				counter = 0;
			}
			timeout = config.getInt("crawler.timeout.value", 0);
			sleepTime = config.getInt("crawler.sleepTime.value", 5000);
			storePath = config.getString("crawler.storePath.value", "WebSite");
			maxBodySize = config.getInt("crawler.maxBodySize.value", 0);
		}

		/**
		 * 构造LOG INFO信息
		 * 
		 * @return
		 * @throws Exception
		 */
		private String setLogInfo() throws Exception {
			String info = "";
			info += "\n< ================ LOG INFO ================ >";
			info += "\n< totally visited:  " + counter;
			info += "\n< download speed:   " + speed + " html/min";
			info += "\n< urlQueue size:    " + urlQueue.size();
			info += "\n< docQueue size:    " + docQueue.size();
			info += "\n< activeThreads:    " + Thread.activeCount();
			info += "\n< ================ LOG INFO ================ >\n";
			if (pExec.isTerminated()) {
				info += "< ============ Task finished!!!! =========== >";
			}
			return info;
		}

		/**
		 * 更新一些正则表达式
		 * 
		 * @throws Exception
		 */
		private void updateRegex() throws Exception {
			boolean isModified = false;
			if (config.containsKey("crawler.acRegex.isUpdated") && config.getBoolean("crawler.acRegex.isUpdated")) {
				acRegex = setRegex("crawler.acRegex.value");
				config.setProperty("crawler.acRegex.isUpdated", "false");
				isModified = true;
			}
			if (config.containsKey("crawler.rjRegex.isUpdated") && config.getBoolean("crawler.rjRegex.isUpdated")) {
				rjRegex = setRegex("crawler.rjRegex.value");
				config.setProperty("crawler.rjRegex.isUpdated", "false");
				isModified = true;
			}
			if (useDateFilter) {
				if (config.containsKey("crawler.dateFilter.isUpdated") && config.getBoolean("crawler.dateFilter.isUpdated")) {
					dateRegex = setRegex("crawler.dateFilter.dateRegex.value");
					startDate = config.getString("crawler.dateFilter.startDate.value");
					endDate = config.getString("crawler.dateFilter.endDate.value");
					config.setProperty("crawler.dateFilter.isUpdated", "false");
					isModified = true;
				}
			}
			if (useParse) {
				isStoreParsedHtml = config.getBoolean("crawler.parse.isStore");
				if (config.getBoolean("crawler.parse.acSelector.isUpdated")) {
					acSelector = config.getList("crawler.parse.acSelector.value", new ArrayList<>());
					config.setProperty("crawler.parse.acSelector.isUpdated", "false");
					isModified = true;
				}
				if (config.getBoolean("crawler.parse.rjSelector.isUpdated")) {
					rjSelector = config.getList("crawler.parse.rjSelector.value", new ArrayList<>());
					config.setProperty("crawler.parse.rjSelector.isUpdated", "false");
					isModified = true;
				}
			}
			if (isModified) {
				config.save();
			}
		}

		/**
		 * 如果useParse的话，将Parse的正文保存到文件
		 * 
		 * @throws Exception
		 */
		private void storeText() throws Exception {
			if (textBuffer.length() == 0) {
				return;
			}
			File textStoreFile = new File(storePath, "text" + File.separator + timestamp);
			synchronized (textBuffer) {
				FileUtils.write(textStoreFile, textBuffer.toString(), encoding, true);
				textBuffer.setLength(0);
			}

		}

		/**
		 * 将已访问队列缓存添加到已访问文件中
		 * 
		 * @throws Exception
		 */
		private void visitedCacheLog() throws Exception {
			if (useParse) {
				synchronized (pVisitedCache) {
					FileUtils.writeLines(pVisitedFile, "utf-8", pVisitedCache, "\n", true);
					pVisitedCache.clear();
				}
			}
			synchronized (upVisitedCache) {
				FileUtils.writeLines(upVisitedFile, "utf-8", upVisitedCache, "\n", true);
				upVisitedCache.clear();
			}
		}

		/**
		 * URL列表的一些操作，包括url列表，url缓存，url文件列表的队列操作
		 * 
		 * @throws Exception
		 */
		private void urlQueueLog() throws Exception {
			boolean isLogged = false;
			int urlQueueSize = urlQueue.size();
			int urlCacheSize = urlCache.size();
			int urlFileQueueSize = urlFileQueue.size();
			if (urlQueueSize < 1000 && urlFileQueueSize == 0) {
				synchronized (Locker) {
					urlQueue.addAll(urlCache);
					urlCache.clear();
				}
			}
			if (urlQueueSize < 1000 && urlFileQueueSize > 0) {
				File urlFile = urlFileQueue.pollFirst();
				urlQueue.addAll(FileUtils.readLines(urlFile, "utf-8"));
				FileUtils.writeLines(currentUrlFile, "utf-8", urlQueue, "\n", false);
				FileUtils.writeLines(urlCacheFile, "utf-8", urlCache, "\n", false);
				isLogged = true;
				if (!urlFile.delete()) {
					logger.warn("delete urlFile " + urlFile.getName() + " failed, please delete manually");
					System.exit(1);
				}
			}
			if (urlCacheSize > maxUrlQueueSize) {
				File urlFile = new File(urlPoolDir, new SimpleDateFormat("yyyyMMddHHmmss").format(new Date()));
				urlFileQueue.addLast(urlFile);
				synchronized (Locker) {
					FileUtils.writeLines(currentUrlFile, "utf-8", urlQueue, "\n", false);
					FileUtils.writeLines(urlFile, "utf-8", urlCache, "\n", false);
					isLogged = true;
					urlCache.clear();
				}
			}
			if (!isLogged) {
				FileUtils.writeLines(currentUrlFile, "utf-8", urlQueue, "\n", false);
				FileUtils.writeLines(urlCacheFile, "utf-8", urlCache, "\n", false);
			}
		}

	}
}
