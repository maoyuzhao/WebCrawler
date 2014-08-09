package edu.bupt.crawler.main;

import java.io.File;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;

import org.apache.commons.io.FileUtils;

public class CutSentence {
	private static final String punctuation = "!\"#$%&')*+,-./:;=>?@]}^_`|~£§­®°³´‑–—―’”•…′″₤€、。〉》・！＃％＆）】＋，．：；？";
	private static final String inpairPunc = "、。，：；？！》）”’】";
	private static final String rightPunc = "“‘﹄『「﹂（【〔《〈﹏[{'\"";

	private static class Pointer {
		private int pos;

		public Pointer() {
			this.pos = 0;
		}

		public void next() {
			this.pos++;
		}

		public int getPos() {
			return pos;
		}

		public void setPos(int pos) {
			this.pos = pos;
		}
	}

	private static HashSet<Character> puncSet;
	private static HashSet<Character> rightPuncSet;
	private static HashSet<Character> inpairPuncSet;
	static {
		puncSet = new HashSet<>();
		for (int i = 0; i < punctuation.length(); i++) {
			puncSet.add(punctuation.charAt(i));
		}
		rightPuncSet = new HashSet<>();
		for (int i = 0; i < rightPunc.length(); i++) {
			rightPuncSet.add(rightPunc.charAt(i));
		}
		inpairPuncSet = new HashSet<>();
		for (int i = 0; i < inpairPunc.length(); i++) {
			inpairPuncSet.add(inpairPunc.charAt(i));
		}
	}

	public static void main(String[] args) throws Exception {
		Iterator<File> ite = FileUtils.iterateFiles(new File("D:\\Work\\wiki\\wiki_data\\text\\OK_raw\\second"), new String[]{"filter"}, false);
		while (ite.hasNext()) {
			File file = ite.next();
			//File file = new File("D:\\Work\\wiki\\wiki_data\\text\\OK_raw\\second\\20140716142316.filter");
			Iterator<String> line_ite = FileUtils.lineIterator(file);
			List<String> list = null;
			StringBuilder builder = new StringBuilder();
			String line = null;
			while (line_ite.hasNext()) {
				line = line_ite.next();
				if (line.startsWith("URL::")) {
					builder.append(line + "\n");
					list = new LinkedList<String>();
					while (true) {
						line = line_ite.next();
						if (line.equals("END::")) {
							break;
						} else {
							list.add(line);
						}
					}
					zhHandler(list, builder);
				}
			}
			FileUtils.write(new File("D:\\Work\\wiki\\wiki_data\\text\\OK_sentence", file.getName() + ".sen"), builder.toString());
		}

	}

	public static boolean isZhTerminal(char ch) {
		return ch == '。' || ch == '？' || ch == '！' || ch == '；';
	}

	public static boolean isPunctuation(char ch) {
		return puncSet.contains(ch);
	}
	
	public static boolean isRightPunctuation(char ch){
		return rightPuncSet.contains(ch);
	}
	
	public static boolean isInpairPunctuation(char ch){
		return inpairPuncSet.contains(ch);
	}

	public static char getChar(char ch) {
		if (ch == '(') {
			return '（';
		} else if (ch == ')') {
			return '）';
		} else {
			return ch;
		}
	}

	public static void ensureNext(Pointer p, String line, StringBuilder builder) {

		if (p.getPos() != line.length() - 1) {
			char ch = line.charAt(p.getPos() + 1);
			ch = getChar(ch);
			if (isPunctuation(ch)) {
				builder.append(ch);
				if (p.getPos() != line.length() - 2) {
					ch = line.charAt(p.getPos() + 2);
					ch = getChar(ch);
					if (isPunctuation(ch)) {
						builder.append(ch);
						p.next();
					}
				}
				p.next();
			}

		}
	}

	public static void zhHandler(List<String> list, StringBuilder builder) {
		for (String line : list) {
			Pointer p = new Pointer();
			for (; p.getPos() < line.length(); p.next()) {
				char ch = line.charAt(p.getPos());
				ch = getChar(ch);
				if (isZhTerminal(ch)) {
					builder.append(ch);
					ensureNext(p, line, builder);
					builder.append('\n');
				} else {
					builder.append(ch);
				}
			}
			if (builder.charAt(builder.length() - 1) != '\n') {
				builder.append("\n");
			}
		}
		builder.append("END::\n");
	}

	public static String searchLeftPunc(String line, Pointer p, char right) {
		StringBuilder builder = new StringBuilder();
		char left = 0;
		switch (right) {
		case '“':
			left = '”';
			break;
		case '‘':
			left = '’';
			break;
		case '﹄':
			left = '﹃';
			break;
		case '『':
			left = '』';
			break;
		case '「':
			left = '」';
			break;
		case '﹂':
			left = '﹁';
			break;
		case '（':
			left = '）';
			break;
		case '【':
			left = '】';
			break;
		case '〔':
			left = ']';
			break;
		case '《':
			left = '》';
			break;
		case '〈':
			left = '〉';
			break;
		case '﹏':
			left = '﹏';
			break;
		case '[':
			left = ']';
			break;
		case '{':
			left = '}';
			break;
		case '\'':
			left = '\'';
			break;
		case '"':
			left = '"';
			break;
		default:
			return null;
		}
		int q = p.getPos();
		int count = 0;
		boolean hasPunc = false;
		for (int i = 1; p.getPos() < line.length(); p.next(), i++) {
			char ch = line.charAt(p.getPos());
			ch = getChar(ch);
			if (ch == right) {
				count++;
			} else if (ch == left) {
				count--;
			}
			if (count != 0 && isInpairPunctuation(ch)) {
				hasPunc = true;
			}
			builder.append(ch);
			if (count == 0) {
				ensureNext(p, line, builder);
				if ((hasPunc && i > 7) || i > 22) {
					return builder.toString();
				} else {
					p.setPos(q);
					return null;
				}
			}
		}
		p.setPos(q);
		return null;
	}
}
