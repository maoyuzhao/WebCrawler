package edu.bupt.crawler.main;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.InputStreamReader;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.concurrent.ConcurrentLinkedQueue;
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
import org.jsoup.nodes.Document;
import org.jsoup.nodes.Element;
import org.jsoup.select.Elements;

import de.l3s.boilerpipe.extractors.ArticleExtractor;
import edu.bupt.util.JudgeContent;
import edu.bupt.util.KVPair;

public class Parser {
	private static final int SINGLE = 0;
	private static final int MULTIPLE = 1;
	private static final int JParser = 2;
	private static final int BParser = 3;
	private static final int MixParser = 4;
	private final Logger logger = Logger.getLogger(Crawler.class);
	private XMLConfiguration config;

	private ConcurrentLinkedQueue<File> fileList; // 所有待Parse文件队列
	private ConcurrentLinkedQueue<String> upUrlList; // 没有抽取出征文的URL列表
	private LinkedBlockingQueue<KVPair<File, KVPair<String, String>>> docQueue; //
	private StringBuffer textBuffer;

	private byte[] pFileLocker;
	private byte[] upFileLocker;

	private String srcPath;
	private String storePath;
	private String encoding;
	private String timestamp;
	private String dateRegex;
	private String startDate;
	private String endDate;
	private int storageType;
	private int parserType;
	private int rThreadNum;
	private int pThreadNum;
	private int maxDocQueueSize;
	private int htmlNumOfEachTimestamp;
	private int parsedCount;
	private int unparsedCount;
	private boolean useDateFilter;
	private List<Object> acSelector;
	private List<Object> rjSelector;

	public static void main(String[] args) {
		Parser parser = new Parser();
		parser.beginParse();
	}

	public void beginParse() {
		/******************************** 配置文件初始化 ********************************/
		PropertyConfigurator.configure("log4j.properties");
		try {
			/**
			 * 设置配置文件重载策略，每10秒钟检测一次
			 */
			config = new XMLConfiguration("parser.xml");
			FileChangedReloadingStrategy strategy = new FileChangedReloadingStrategy();
			strategy.setRefreshDelay(10000);
			config.setReloadingStrategy(strategy);
		} catch (Exception e) {
			logger.error(e.getMessage(), e);
		}
		/********************************* 参数初始化 **********************************/
		srcPath = config.getString("parser.srcPath.value");
		storePath = config.getString("parser.storePath.value");
		if (config.getString("parser.storageType.value").equalsIgnoreCase("single")) {
			storageType = Parser.SINGLE;
			rThreadNum = config.getInt("parser.readThreadNum.value", 20);
			pThreadNum = config.getInt("parser.parseThreadNum.value", 20);
		} else {
			storageType = Parser.MULTIPLE;
			rThreadNum = config.getInt("parser.readThreadNum.value", 40);
			pThreadNum = config.getInt("parser.parseThreadNum.value", 4);
		}
		String parserTypeString = config.getString("parser.parserType.value");
		if (parserTypeString.equalsIgnoreCase("jparser")) {
			this.parserType = Parser.JParser;
		} else if (parserTypeString.equalsIgnoreCase("bparser")) {
			this.parserType = Parser.BParser;
		} else if (parserTypeString.equalsIgnoreCase("mixparser")) {
			this.parserType = Parser.MixParser;
		}
		acSelector = config.getList("parser.acSelector.value");
		rjSelector = config.getList("parser.rjSelector.value");
		timestamp = new SimpleDateFormat("yyyyMMddHHmmss").format(new Date());
		encoding = config.getString("parser.encoding.value", "utf-8");
		useDateFilter = config.getBoolean("parser.dateFilter.useDateFilter", false);
		if (useDateFilter) {
			dateRegex = setRegex("parser.dateFilter.dateRegex.value");
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
		htmlNumOfEachTimestamp = config.getInt("parser.htmlNumOfEachTimestamp.value", 10000);
		parsedCount = 0;
		unparsedCount = 0;
		fileList = new ConcurrentLinkedQueue<>(FileUtils.listFiles(new File(srcPath), null, true));
		maxDocQueueSize = config.getInt("parser.maxDocQueueSize.value", 300);
		docQueue = new LinkedBlockingQueue<>(maxDocQueueSize);
		upUrlList = new ConcurrentLinkedQueue<>();
		textBuffer = new StringBuffer();
		pFileLocker = new byte[0];
		upFileLocker = new byte[0];
		try {
			startThread();
		} catch (Exception e) {
			logger.error(e.getMessage(), e);
		}
	}

	private void startThread() throws Exception {
		ScheduledExecutorService scheExec = Executors.newScheduledThreadPool(1);
		ExecutorService rExec = Executors.newFixedThreadPool(rThreadNum);
		ExecutorService pExec = Executors.newFixedThreadPool(pThreadNum);
		scheExec.scheduleAtFixedRate(new Loger(pExec), 5, 5, TimeUnit.SECONDS);

		rExec.execute(new Reader());
		pExec.execute(new Processer(rExec));
		while (--rThreadNum > 0 && --pThreadNum > 0) {
			rExec.execute(new Reader());
			pExec.execute(new Processer(rExec));
			Thread.sleep(1000);
		}
		while (rThreadNum-- > 0) {
			rExec.execute(new Reader());
			Thread.sleep(1000);
		}
		while (pThreadNum-- > 0) {
			pExec.execute(new Processer(rExec));
			Thread.sleep(1000);
		}
		rExec.shutdown();
		pExec.shutdown();
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

	public class Reader implements Runnable {

		public void readSingle(File file) throws Exception {
			BufferedReader br = new BufferedReader(new InputStreamReader(new FileInputStream(file), encoding));
			String sizeString = null;
			String url = null;
			char[] cbuf = null;
			while ((sizeString = br.readLine()) != null) {
				url = br.readLine();
				int size = Integer.valueOf(sizeString);
				cbuf = new char[size];
				br.read(cbuf, 0, size);
				putPairs(new KVPair<>(file, new KVPair<>(url, new String(cbuf))));
			}
			br.close();
		}

		public void readMutiple(File file) throws Exception {
			BufferedReader br = new BufferedReader(new InputStreamReader(new FileInputStream(file), encoding));
			int size = Integer.valueOf(br.readLine());
			String url = br.readLine();
			char[] cbuf = new char[size];
			br.read(cbuf, 0, size);
			br.close();
			putPairs(new KVPair<>(file, new KVPair<>(url, new String(cbuf))));
		}

		private void putPairs(KVPair<File, KVPair<String, String>> pairs) throws Exception {
			while (true) {
				if (docQueue.offer(pairs, 200, TimeUnit.MILLISECONDS)) {
					break;
				}
			}
		}

		@Override
		public void run() {
			File file = null;
			while (true) {
				file = fileList.poll();
				if (file == null) {
					break;
				}
				try {
					switch (storageType) {
					case Parser.SINGLE:
						readSingle(file);
						break;
					case Parser.MULTIPLE:
						readMutiple(file);
						break;
					}
				} catch (Exception e) {
					logger.error(e.getMessage(), e);
				}
			}
			logger.info(Thread.currentThread().getName() + "(Reader) is finished");
		}

	}

	public class Processer implements Runnable {

		private ExecutorService rExec; // reader线程池

		public Processer(ExecutorService rExec) {
			this.rExec = rExec;
		}

		public Processer() {
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
		private boolean parse(String html, String url) throws Exception {
			boolean isParsed = false;
			String content = null;
			switch (parserType) {
			case JParser:
				content = jsoupParser(Jsoup.parse(html), acSelector, rjSelector);
				break;
			case BParser:
				content = boilerpipeParser(html);
				break;
			case MixParser:
				content = jsoupParser(Jsoup.parse(html), acSelector, rjSelector);
				if (content == null) {
					content = boilerpipeParser(html);
				}
				break;
			default:
				break;
			}
			if (content != null) {
				isParsed = true;
				content = "URL::" + url + "\n" + content + "\nEND::\n";
				textBuffer.append(content);
			} else {
				isParsed = false;
				upUrlList.add(url);
			}
			return isParsed;
		}

		/**
		 * 获取合法的Document-url Paris
		 * 
		 * @return
		 * @throws Exception
		 */
		private KVPair<File, KVPair<String, String>> getDocument() throws Exception {
			KVPair<File, KVPair<String, String>> pairs = null;
			while (true) {
				pairs = docQueue.poll(200, TimeUnit.MILLISECONDS);
				if (pairs == null) {
					if (rExec.isTerminated()) {
						break;
					} else {
						continue;
					}
				} else if (useDateFilter) {
					if (JudgeContent.isDateLegal(pairs.getValue().getValue(), startDate, endDate, dateRegex)) {
						break;
					} else {
						continue;
					}
				} else {
					break;
				}
			}
			return pairs;
		}

		private void storeSingle(File file, String storePath, String url, final String html, boolean isParsed) throws Exception {
			String content = html.length() + "\n" + url + "\n" + html;
			File storFile = null;
			if (isParsed) {
				storFile = new File(storePath, "p" + File.separator + "_" + timestamp);
				synchronized (pFileLocker) {
					FileUtils.write(storFile, content, encoding, true);
					parsedCount++;
				}
			} else {
				storFile = new File(storePath, "up" + File.separator + "_" + timestamp);
				synchronized (upFileLocker) {
					FileUtils.write(storFile, content, encoding, true);
					unparsedCount++;
				}
			}
		}

		private void storeMultiple(File file, String storePath, String url, final String html, boolean isParsed) throws Exception {
			if (isParsed) {
				File destFile = new File(storePath, "p" + File.separator + timestamp + File.separator + file.getName());
				if (!destFile.getParentFile().exists()) {
					destFile.getParentFile().mkdirs();
				}
				if (!file.renameTo(destFile)) {
					System.err.println("Moving file " + file.getAbsolutePath() + " failed");
				}
				synchronized (pFileLocker) {
					parsedCount++;
				}
			} else {
				synchronized (upFileLocker) {
					unparsedCount++;
				}
			}
		}

		private void store(File file, String url, final String html, boolean isParsed) throws Exception {
			switch (storageType) {
			case Parser.SINGLE:
				storeSingle(file, storePath, url, html, isParsed);
				break;
			case Parser.MULTIPLE:
				storeMultiple(file, storePath, url, html, isParsed);
				break;
			default:
				break;
			}
		}

		@Override
		public void run() {
			KVPair<File, KVPair<String, String>> pairs = null;
			String url = null;
			String html = null;
			File file = null;
			boolean isParsed = false;
			while (true) {
				try {
					pairs = getDocument();
					if (pairs == null) {
						break;
					}
					file = pairs.getKey();
					url = pairs.getValue().getKey();
					html = pairs.getValue().getValue();
					isParsed = parse(html, url);
					store(file, url, html, isParsed);
				} catch (Exception e) {
					logger.warn(e.getMessage(), e);
				}
			}
			logger.info(Thread.currentThread().getName() + " (parser) is finished");
		}
	}

	public class Loger implements Runnable {
		private ExecutorService pExec;
		private int lastCounter;
		private int speed;
		private int times;

		public Loger(ExecutorService pExec) {
			this.pExec = pExec;
			lastCounter = 0;
			times = 0;
		}

		@Override
		public void run() {
			try {
				storeText();
				updateTimestamp();
				updateRegex();
				logger.info(setLogInfo());
				if (pExec.isTerminated()) {
					System.out.println("Parser has completed!!!");
					System.exit(0);
				}
			} catch (Exception e) {
				logger.error(e.getMessage(), e);
			}
		}

		private String setLogInfo() throws Exception {
			String info = "";
			info += "\n< ================LOG INFO================ >";
			info += "\n< Speed:           " + speed + "doc/sec";
			info += "\n< Parsed:          " + parsedCount;
			info += "\n< Unparsed:        " + unparsedCount;
			info += "\n< docQueue:        " + docQueue.size();
			info += "\n< activeThreads:   " + Thread.activeCount();
			info += "\n< ================LOG INFO================ >\n";
			return info;
		}

		private void storeText() throws Exception {
			if (textBuffer.length() == 0) {
				return;
			}
			File textFile = new File(storePath, "text" + File.separator + timestamp);
			File unparsedUrlFile = new File(storePath, "upUrl.txt");
			synchronized (textBuffer) {
				FileUtils.write(textFile, textBuffer.toString(), encoding, true);
				textBuffer.setLength(0);
			}
			synchronized (upUrlList) {
				FileUtils.writeLines(unparsedUrlFile, "utf-8", upUrlList, "\n", true);
				upUrlList.clear();
			}
		}

		private void updateTimestamp() {
			int total = unparsedCount + parsedCount;
			int count = total - lastCounter;
			times++;
			speed = count / (times * 5);
			if (count >= htmlNumOfEachTimestamp) {
				timestamp = new SimpleDateFormat("yyyyMMddHHmmss").format(new Date());
				lastCounter = total;
				times = 0;
			}
		}

		private void updateRegex() throws Exception {
			boolean isModified = false;
			if (config.getBoolean("parser.acSelector.isUpdated")) {
				acSelector = config.getList("parser.acSelector.value");
				config.setProperty("parser.acSelector.isUpdated", "false");
				isModified = true;
			}
			if (config.getBoolean("parser.rjSelector.isUpdated")) {
				rjSelector = config.getList("parser.rjSelector.value", new ArrayList<Object>());
				config.setProperty("parser.rjSelector.isUpdated", "false");
				isModified = true;
			}
			if (isModified) {
				config.save();
			}
		}
	}
}
