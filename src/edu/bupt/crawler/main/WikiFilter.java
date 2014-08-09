package edu.bupt.crawler.main;

import java.io.File;
import java.net.URLDecoder;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Set;

import org.apache.commons.io.FileUtils;
import org.jsoup.helper.StringUtil;

import com.spreada.utils.chinese.ZHConverter;

public class WikiFilter {
	public static void main(String[] args) throws Exception {
		ZHConverter converter = ZHConverter.getInstance(ZHConverter.SIMPLIFIED);
		StringBuilder builder = new StringBuilder();
		Set<String> titleSet = new HashSet<>();
		String url = null;
		String title = null;
		Iterator<File> ite = FileUtils.iterateFiles(new File("D:\\Work\\wiki\\wiki_data\\text\\OK_raw\\second"), null, false);
		while (ite.hasNext()) {
			File file = ite.next();
			boolean append = false;
			Iterator<String> line_ite = FileUtils.lineIterator(file);
			String line = null;
			while (line_ite.hasNext()) {
				line = line_ite.next().trim();
				if (line.length() == 0) {
					continue;
				}
				if (line.startsWith("URL::")) {
					url = converter.convert(URLDecoder.decode(line.split("::")[1], "utf-8"));
					title = url.substring(url.indexOf("/zh-cn/") + 7);
					if (titleSet.contains(title)) {
						while (!line.equals("END::")) {
							line = line_ite.next();
						}
					} else {
						builder.append("URL::" + url + "\n");
						titleSet.add(title);
						append = true;
					}
				}else if (append && StringUtil.in(line, "相关条目", "外部链接", "外部连接", "参考资料", "参考文献", "延伸阅读", "参见", "相关项目", "参考")) {
					append = false;
				} else if (append || line.equals("END::") || line.startsWith("Category::")) {
					line = line.replaceAll("\\[.*?]", "");
					builder.append(line + "\n");
				}
			}
			FileUtils.write(new File(file.getAbsolutePath() + ".filter"), builder.toString());
			builder.setLength(0);
		}
		FileUtils.writeLines(new File("wikiTitle"), titleSet);
	}
}
