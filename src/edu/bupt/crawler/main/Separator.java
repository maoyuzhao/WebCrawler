package edu.bupt.crawler.main;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.InputStreamReader;
import java.io.OutputStreamWriter;

import org.apache.commons.configuration.XMLConfiguration;
import org.apache.log4j.Logger;
import org.apache.log4j.PropertyConfigurator;

public class Separator {
	private Logger logger;
	private XMLConfiguration config;

	private String sourceFilename;
	private String storeFolderPath;
	private String sourceEncoding;
	private String destEncoding;
	private int leastCharNum;
	private long fixedSize;

	public static void main(String[] args) {
		Separator separator = new Separator();
		try {
			separator.beginSeparator();
		} catch (Exception e) {
			separator.logger.error(e.getMessage(), e);
			System.err.println(e.getMessage());
			System.err.println("The process has been closed by exception!");
			System.exit(1);
		}
	}

	void beginSeparator() {
		logger = Logger.getLogger(Separator.class);
		PropertyConfigurator.configure("log4j.properties");
		try {
			config = new XMLConfiguration("separator.xml");
		} catch (Exception e) {
			logger.error(e.getMessage(), e);
			System.out.println(e.getMessage());
			System.out.println("The process has been closed by exception!");
			System.exit(0);
		}
		sourceFilename = config.getString("separator.sourceFilename.value");
		storeFolderPath = config.getString("separator.storeFolderPath.value");
		sourceEncoding = config.getString("separator.sourceEncoding.value");
		destEncoding = config.getString("separator.destEncoding.value");
		leastCharNum = config.getInt("separator.leastCharNum.value", 30);
		fixedSize = 30000000l;
		try {
			separate();
		} catch (Exception e) {
			e.printStackTrace();
		}
	}

	public void separate() throws Exception {
		File srcFile = new File(sourceFilename);
		File discardFile = new File(storeFolderPath, "discard_text");
		if (srcFile.length() < fixedSize) {
			System.out.println("dont't need to separate");
			return;
		}
		BufferedReader br = new BufferedReader(new InputStreamReader(new FileInputStream(srcFile), sourceEncoding));
		BufferedWriter discardBW = new BufferedWriter(new OutputStreamWriter(new FileOutputStream(discardFile), destEncoding));
		System.out.println("\"" + srcFile.getName() + "\"　has loaded ");
		System.out.println("...");
		StringBuilder sbTemp = new StringBuilder();
		StringBuilder sb = new StringBuilder();
		String line = null;
		String url = null;
		int i = 1;
		while ((line = br.readLine()) != null) {
			if (line.startsWith("URL::")) {
				url = line;
			} else if (line.equals("END::")) {
				if (sbTemp.length() > leastCharNum) {
					sb.append(url + "\n");
					sb.append(sbTemp);
					sb.append("END::\n");
				} else if (sbTemp.length() != 0) {
					discardBW.write(url + "\n" + sbTemp.toString() + "END::\n");
				}
				sbTemp.setLength(0);
				if (sb.length() > fixedSize) {
					File outputFile = new File(storeFolderPath, srcFile.getName() + ".part" + i);
					BufferedWriter bw = new BufferedWriter(new OutputStreamWriter(new FileOutputStream(outputFile), destEncoding));
					bw.write(sb.toString());
					sb.setLength(0);
					i++;
					bw.close();
					System.out.println("\"" + outputFile.getName() + "\" has generated...");
				}
			} else {
				sbTemp.append(line + "\n");
			}
		}
		if (sb.length() != 0) {
			File outputFile = new File(storeFolderPath, srcFile.getName() + ".part" + i);
			BufferedWriter bw = new BufferedWriter(new OutputStreamWriter(new FileOutputStream(outputFile), destEncoding));
			bw.write(sb.toString());
			sb.setLength(0);
			i++;
			bw.close();
			System.out.println("\"" + outputFile.getName() + "\" has generated...");
		}
		System.out.println("...");
		System.out.println("compelete!");
		br.close();
		discardBW.close();
	}
}
