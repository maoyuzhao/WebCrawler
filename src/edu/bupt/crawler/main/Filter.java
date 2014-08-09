package edu.bupt.crawler.main;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.InputStreamReader;
import java.io.OutputStreamWriter;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;

import org.apache.commons.configuration.XMLConfiguration;
import org.apache.commons.io.FileUtils;
import org.apache.log4j.Logger;
import org.apache.log4j.PropertyConfigurator;

import edu.bupt.util.BloomFilter;
import edu.bupt.util.KVPair;

public class Filter {
	private Logger logger;
	private XMLConfiguration config;

	private File sourceFile;
	private File storeFolder;
	private String encoding;
	private BloomFilter<String> bFilter;
	private Map<Character, Character> convertTable;
	private Map<Character, Integer> illegalCounter;
	private Set<Character> legalCharSet;
	private Set<Character> spaceCharSet;
	private int leastChineseCharNum;
	private int multiAppearNum;
	private float chineseCharProportion;
	private boolean isStoreRepeat;
	private boolean isStoreBad;
	private boolean isSotreError;

	public static void main(String[] args) {
		Filter filter = new Filter();
		try {
			filter.beginFilter();
		} catch (Exception e) {
			filter.logger.error(e.getMessage(), e);
			System.out.println(e.getMessage());
			System.out.println("The process has been closed by exception!");
			System.exit(0);
		}
	}

	public void beginFilter() {
		logger = Logger.getLogger(Filter.class);
		PropertyConfigurator.configure("log4j.properties");
		try {
			config = new XMLConfiguration("filter.xml");
		} catch (Exception e) {
			logger.error(e.getMessage(), e);
			System.out.println(e.getMessage());
			System.out.println("The process has been closed by exception!");
			System.exit(0);
		}
		sourceFile = new File(config.getString("filter.sourceFolderPath.value"));
		storeFolder = new File(config.getString("filter.storeFolderPath.value"));
		encoding = config.getString("filter.encoding.value");
		isStoreRepeat = config.getBoolean("filter.isStoreRepeat.value", false);
		isStoreBad = config.getBoolean("filter.isStoreBad.value", true);
		isSotreError = config.getBoolean("filter.isSotreError.value", true);
		double errorRate = config.getDouble("filter.bloomFilter.errorRate.value", 0.0001);
		int expectedNumberOfFilterElements = config.getInt("filter.bloomFilter.expectedNumberOfElements.value", 1600000000);
		bFilter = new BloomFilter<>(errorRate, expectedNumberOfFilterElements);
		leastChineseCharNum = config.getInt("filter.chineseCharNumberThreshold.value", 5);
		multiAppearNum = config.getInt("filter.chineseCharNumberThreshold.value", 5);
		chineseCharProportion = config.getFloat("filter.chineseCharProportion.value", 0.75f);
		illegalCounter = new HashMap<>();
		try {
			legalCharSet = loadSet("filter" + File.separator + "LegalChars.txt");
			String spaceChar = " \t\n\b\f\r";
			spaceCharSet = new HashSet<Character>();
			for (int i = 0; i < spaceChar.length(); i++) {
				spaceCharSet.add(spaceChar.charAt(i));
			}
			convertTable = loadMap("filter" + File.separator + "ConvertTable.txt");
			System.out.println("Congfig files have been loaded successfully!");
			createFiles();
			System.out.println("the process is loading text files, please wait");
			System.out.println("...");
			fileHandler();
			outputIllegalChars();
		} catch (Exception e) {
			logger.error(e.getMessage(), e);
			System.out.println(e.getMessage());
			System.exit(0);
		}

	}

	private Set<Character> loadSet(String filename) throws Exception {
		Set<Character> set = new HashSet<>();
		Iterator<String> lineIte = FileUtils.lineIterator(new File(filename), "utf-8");
		while (lineIte.hasNext()) {
			String chars = lineIte.next();
			for (int i = 0; i < chars.length(); i++) {
				set.add(chars.charAt(i));
			}
		}
		return set;
	}

	private Map<Character, Character> loadMap(String filename) throws Exception {
		Map<Character, Character> table = new HashMap<>();
		List<String> lines = FileUtils.readLines(new File(filename), "utf-8");
		for (String line : lines) {
			if (line != null) {
				if (line.length() != 2) {
					throw new Exception(filename + " is a bad config file!");
				} else {
					table.put(line.charAt(0), line.charAt(1));
				}
			}
		}
		return table;
	}

	private void createFiles() throws Exception {
		if (!sourceFile.exists()) {
			throw new Exception("The source folder doesn't exist!");
		}
		if (!sourceFile.canRead()) {
			throw new Exception("Can not access the source folder, permission denied!");
		}
		if (!sourceFile.isDirectory()) {
			throw new Exception("The source is not a directory!");
		}
		if (!storeFolder.exists()) {
			storeFolder.mkdir();
		} else if (!storeFolder.canWrite()) {
			throw new Exception("Can not access the target folder, permission denied!");
		}
	}

	private void outputIllegalChars() throws Exception {
		ArrayList<Entry<Character, Integer>> list = new ArrayList<>(illegalCounter.entrySet());
		Collections.sort(list, (o1, o2)-> o2.getValue().compareTo(o1.getValue()));
		FileUtils.writeLines(new File(storeFolder, "illegal.dic"), list);
		System.out.println("\nThe text has bean handled successfully!");
	}

	private void fileHandler() throws Exception {
		String line = null;
		File inputFile = null;
		String currentFilename = null;
		BufferedReader input = null;

		BufferedWriter acOutput = null;
		BufferedWriter repOutput = null;
		BufferedWriter badOutput = null;
		BufferedWriter errOutput = null;

		CleanLine cleanLine = null;
		KVPair<Character, CleanLine> res = null;

		Iterator<File> fileIte = FileUtils.iterateFiles(sourceFile, null, true);
		System.out.println("text files load completed!");

		acOutput = new BufferedWriter(new OutputStreamWriter(new FileOutputStream(new File(storeFolder, "remained_text")), encoding));
		while (fileIte.hasNext()) {
			inputFile = fileIte.next();
			currentFilename = inputFile.getName();
			System.out.println("begin to filter file " + currentFilename + "...");

			input = new BufferedReader(new InputStreamReader(new FileInputStream(inputFile), encoding));
			if (isStoreRepeat) {
				File repFile = new File(storeFolder, "repeat" + File.separator + currentFilename + ".rep");
				if (!repFile.getParentFile().exists()) {
					repFile.getParentFile().mkdirs();
				}
				repOutput = new BufferedWriter(new OutputStreamWriter(new FileOutputStream(repFile), encoding));
			}
			if (isStoreBad) {
				File badFile = new File(storeFolder, "bad" + File.separator + currentFilename + ".bad");
				if (!badFile.getParentFile().exists()) {
					badFile.getParentFile().mkdirs();
				}
				badOutput = new BufferedWriter(new OutputStreamWriter(new FileOutputStream(badFile), encoding));
			}
			if (isSotreError) {
				File errFile = new File(storeFolder, "error" + File.separator + currentFilename + ".err");
				if (!errFile.getParentFile().exists()) {
					errFile.getParentFile().mkdirs();
				}
				errOutput = new BufferedWriter(new OutputStreamWriter(new FileOutputStream(errFile), encoding));
			}

			while ((line = input.readLine()) != null) {
				if (line.length() < leastChineseCharNum) {
					continue;
				} else if (line.startsWith("URL::")) {
					acOutput.write(line + "\n");
				} else if (line.equals("END::")) {
					acOutput.write(line + "\n");
				} else {
					res = lineHandler(line);
					char illegalChar = res.getKey();
					cleanLine = res.getValue();
					if (isLegalLine(cleanLine)) {
						if (!bFilter.contains(cleanLine.m_chineseSection)) {
							bFilter.add(cleanLine.m_chineseSection);
							acOutput.write(cleanLine.m_possessiveLine + "\n");
						} else if (isStoreRepeat) {
							repOutput.write(line + "\n");
						}
					} else {
						if (illegalChar == '\0') {
							badOutput.write(line + "\n");
						} else {
							errOutput.write(illegalChar + "\t" + line + "\n");
						}
					}

				}
			}

			input.close();

			if (isStoreRepeat) {
				repOutput.close();
			}
			if (isStoreBad) {
				badOutput.close();
			}
			if (isSotreError) {
				errOutput.close();
			}
		}
		acOutput.close();
	}

	public KVPair<Character, CleanLine> lineHandler(String line) {
		char ch = '\0';
		char lastCh = '\0';
		int counter = 0;
		StringBuilder chineseBuilder = new StringBuilder();
		StringBuilder builder = new StringBuilder();
		for (int i = 0; i < line.length(); i++) {
			ch = line.charAt(i);
			ch = converts(ch);
			if (lastCh == ch) {
				counter++;
				if (counter >= multiAppearNum) {
					return new KVPair<Character, CleanLine>(ch, null);
				}
			} else if (isSpaceChar(ch)) {
				continue;
			} else if (isChineseChar(ch)) {
				builder.append(ch);
				chineseBuilder.append(ch);
			} else if (isLegalChar(ch)) {
				builder.append(ch);
			} else {
				counterPlusOne(ch);
				return new KVPair<Character, CleanLine>(ch, null);

			}
		}
		return new KVPair<>('\0', new CleanLine(chineseBuilder.toString(), builder.toString()));
	}

	private boolean isLegalLine(CleanLine cleanLine) {
		if (cleanLine == null) {
			return false;
		}
		int clength = cleanLine.m_chineseSection.length();
		int plength = cleanLine.m_possessiveLine.length();
		boolean flag = clength > leastChineseCharNum && (clength > 50 || (float) clength / (float) plength > chineseCharProportion);
		return flag;
	}

	private boolean isChineseChar(char ch) {
		Character.UnicodeBlock ub = Character.UnicodeBlock.of(ch);
		if (ub == Character.UnicodeBlock.CJK_UNIFIED_IDEOGRAPHS 
		 || ub == Character.UnicodeBlock.CJK_COMPATIBILITY_IDEOGRAPHS
		 || ub == Character.UnicodeBlock.CJK_UNIFIED_IDEOGRAPHS_EXTENSION_A) {
			return true;
		}
		return false;
	}

	private boolean isLegalChar(char ch) {
		Character.UnicodeBlock ub = Character.UnicodeBlock.of(ch);
		if (Character.isAlphabetic(ch) 
		 || Character.isDigit(ch) 
		 || legalCharSet.contains(ch)
		 || ub == Character.UnicodeBlock.CJK_SYMBOLS_AND_PUNCTUATION 
		 || ub == Character.UnicodeBlock.HALFWIDTH_AND_FULLWIDTH_FORMS
		 || ub == Character.UnicodeBlock.GENERAL_PUNCTUATION) {
			return true;
		} else {
			return false;
		}

	}

	private boolean isSpaceChar(char ch) {
		if (spaceCharSet.contains(ch)) {
			return true;
		}
		return false;
	}

	private char converts(char ch) {
		Character rch = convertTable.get(ch);
		if (rch == null) {
			return ch;
		} else {
			return rch;
		}
	}

	private void counterPlusOne(char ch) {
		if (illegalCounter.containsKey(ch)) {
			illegalCounter.put(ch, illegalCounter.get(ch) + 1);
		} else {
			illegalCounter.put(ch, 1);
		}
	}

	private class CleanLine {
		public CleanLine(String chineseSection, String possessiveLine) {
			m_chineseSection = chineseSection;
			m_possessiveLine = possessiveLine;
		}

		String m_chineseSection;
		String m_possessiveLine;
	}

}
