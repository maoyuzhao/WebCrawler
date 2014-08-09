package edu.bupt.test;


import java.io.File;
import java.net.URL;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.LinkedBlockingQueue;

import org.apache.commons.configuration.XMLConfiguration;
import org.apache.commons.io.FileUtils;
import org.jsoup.Jsoup;
import org.jsoup.Connection.Response;
import org.jsoup.nodes.Document;
import org.jsoup.nodes.Element;
import org.jsoup.select.Elements;

import de.l3s.boilerpipe.extractors.ArticleExtractor;
import edu.bupt.crawler.main.Crawler;
import edu.bupt.util.KVPair;
import edu.bupt.util.NetManager;
import edu.bupt.util.RegexManager;


public class Test {
	public static void main(String[] args) throws Exception {
		LinkedBlockingQueue<KVPair<String, KVPair<Document, Document>>> docQueue = new LinkedBlockingQueue<>();
		String regex = "http://techcrunch.cn/\\d{4}/\\d{2}/\\d{2}/([^/]+).*";
		String urlZH = "http://techcrunch.cn/2014/05/06/kabbage-50m/";
		String urlEN = "http://techcrunch.com/" + RegexManager.group(regex, urlZH, 1);
		Document docZH = downloadHtml(urlZH); // 通过URL下载页面
		Document docEN = downloadHtml(urlEN);
		if (docZH != null && docEN != null) {
			KVPair<String, KVPair<Document, Document>> paire = new KVPair<>(urlZH, new KVPair<>(docZH, docEN));
			docQueue.offer(paire);
			FileUtils.write(new File("ZH"), docZH.html());
			FileUtils.write(new File("EN"), docEN.html());
		}
		KVPair<String, KVPair<Document, Document>> pair = docQueue.poll();
		String urlZH_ = pair.getKey();
		Document docZH_ = pair.getValue().getKey();
		Document docEN_ = pair.getValue().getValue();
		String ZH = NetManager.getSpecialText(docZH_.html(), "div#module-post-detail div.body-copy p", null);
		String EN = NetManager.getSpecialText(docEN_.html(), "div.article-entry p", null);
		System.out.println(ZH);
		System.out.println(EN);
	}
	public static Document downloadHtml(String url) throws Exception {
		Document doc = null;
		Response rep = Jsoup.connect(url)
				.userAgent("Mozilla/5.0 (Windows NT 6.1; WOW64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/28.0.1500.72 Safari/537.36")
				.ignoreHttpErrors(true).referrer(url).timeout(0).execute();
		int statusCode = rep.statusCode();
		if (statusCode == 200) {
			doc = rep.parse();
		} else {
			System.err.printf("%-16s %-10s\t%s\n", Thread.currentThread().getName(), statusCode, url);
		}
		return doc;
	}
	public static String jsoupParser(Document docZH, Document docEN, List<?> acSelector, List<?> rjSelector) {
		StringBuilder parsedContent = new StringBuilder();
		boolean flag = false;
		String temp = null;
		for (Object rs : rjSelector) {
			docZH.select(rs.toString()).remove();
		}
		for (Object rs : rjSelector) {
			docEN.select(rs.toString()).remove();
		}
		docZH.select("br").append("BR2N");
		docZH.select("p").append("BR2N");
		docEN.select("br").append("BR2N");
		docEN.select("p").append("BR2N");
		for (Object as : acSelector) {
			Elements elementsZH = docZH.select(as.toString());
			Elements elementsEN = docEN.select(as.toString());
			if (elementsZH.isEmpty() || elementsEN.isEmpty()) {
				continue;
			} else {
				if (!flag) {
					flag = true;
				}
				for (Element element : elementsZH) {
					temp = element.text().replaceAll("BR2N", "\n").trim();
					if (temp.length() > 0) {
						parsedContent.append(temp + "\n");
					}
				}
				for (Element element : elementsEN) {
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
}
