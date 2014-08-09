package edu.bupt.test;

import java.io.File;
import java.io.IOException;
import java.util.Iterator;

import org.apache.commons.io.FileUtils;

public class Test1 {
	public static void main(String[] args) throws Exception {
		Iterator<File> ite = FileUtils.iterateFiles(new File("D:\\Work\\wiki\\wiki_data\\text\\OK_final"), new String[] { "sen" }, false);
		StringBuilder builder1 = new StringBuilder();
		StringBuilder builder2 = new StringBuilder();
		int count = 0;
		int num = 0;
		while (ite.hasNext()) {
			File file_sen = ite.next();
			File file_tag = new File(file_sen.getAbsolutePath() + ".tag");
			Iterator<String> line_ite1 = FileUtils.lineIterator(file_sen);
			Iterator<String> line_ite2 = FileUtils.lineIterator(file_tag);
			while (line_ite1.hasNext()) {
				count++;
				builder1.append(line_ite1.next() + "\n");
				builder2.append(line_ite2.next() + "\n" + line_ite2.next() + "\n" + line_ite2.next() + "\n" + line_ite2.next() + "\n");
				if (count == 300000) {
					FileUtils.write(new File("D:\\Work\\wiki\\wiki_data\\text\\OK_final", num + ".sen"), builder1.toString());
					FileUtils.write(new File("D:\\Work\\wiki\\wiki_data\\text\\OK_final", num + ".tag"), builder2.toString());
					builder1.setLength(0);
					builder2.setLength(0);
					num ++;
					count = 0;
				}
			}
		}
		FileUtils.write(new File("D:\\Work\\wiki\\wiki_data\\text\\OK_final", num + ".sen"), builder1.toString());
		FileUtils.write(new File("D:\\Work\\wiki\\wiki_data\\text\\OK_final", num + ".tag"), builder2.toString());
		builder1.setLength(0);
		builder2.setLength(0);
	}
}
