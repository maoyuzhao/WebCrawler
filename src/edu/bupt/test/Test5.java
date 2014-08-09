package edu.bupt.test;

import java.io.File;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

import org.apache.commons.io.FileUtils;

import edu.bupt.util.BloomFilter;

public class Test5 {
	private static String url;
	private static String title;
	private static String cat;

	public static void main(String[] args) throws Exception {
		Iterator<File> ite = FileUtils.iterateFiles(new File("D:\\Work\\wiki\\wiki_data\\text\\OK_sentence"), null, true);
		BloomFilter<String> bFilter = new BloomFilter<>(0.0001, 1600000000);
		StringBuilder builder1 = new StringBuilder();
		StringBuilder builder2 = new StringBuilder();
		List<String> list = new ArrayList<>();
		while (ite.hasNext()) {
			File file = ite.next();
			Iterator<String> line_ite = FileUtils.lineIterator(file);
			while (line_ite.hasNext()) {
				String line = line_ite.next().trim();
				if (line.length() == 1) {
					continue;
				}
				if (line.startsWith("URL::")) {
					url = line.split("::")[1];
					title = url.substring(url.indexOf("/zh-cn/") + 7);
				} else if (line.startsWith("Category::")) {
					cat = line.split("::")[1].replaceAll(",", "|");
				} else if (line.equals("END::")) {
					for (String s : list) {
						builder1.append(s + "\n");
						builder2.append(title + "\n" + cat + "\nwikipedia\n\n");
					}
					list.clear();
				} else if (!bFilter.contains(line)) {
					bFilter.add(line);
					list.add(line);
				}

			}
			FileUtils.write(new File("D:\\Work\\wiki\\wiki_data\\text\\OK_final", file.getName()), builder1.toString());
			FileUtils.write(new File("D:\\Work\\wiki\\wiki_data\\text\\OK_final", file.getName() + ".tag"), builder2.toString());
			builder1.setLength(0);
			builder2.setLength(0);
		}
	}

}