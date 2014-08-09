package edu.bupt.util;

import java.net.URL;
import java.util.Collection;
import java.util.HashSet;

import org.jsoup.Jsoup;
import org.jsoup.Connection.Response;
import org.jsoup.helper.StringUtil;
import org.jsoup.nodes.Document;
import org.jsoup.nodes.Element;
import org.jsoup.nodes.Node;
import org.jsoup.nodes.TextNode;
import org.jsoup.select.Elements;
import org.jsoup.select.NodeTraversor;
import org.jsoup.select.NodeVisitor;

public class NetManager {
	private NetManager() {
	}

	public static void main(String[] args) throws Exception {
		URL url = new URL("http://zh.wikipedia.org/wiki/%E7%B5%90%E6%A7%8B%E5%8C%96%E7%A8%8B%E5%BC%8F%E8%A8%AD%E8%A8%88");
		Document doc = Jsoup.parse(url, 0);
		//System.out.println(extractURLs("http://auto.sina.com.cn", "^http://[^/]*auto.sina.com.cn.*"));
		System.out.println(getSpecialText(doc.html(), "div#mw-content-text", new String[] { "table", ".toc", ".mw-editsection", ".tumb" }));
	}

	public static Collection<String> extractURLs(String url, String regex) throws Exception {
		Collection<String> list = new HashSet<>();
		Document doc = null;
		Response response = Jsoup.connect(url.replaceAll(" ", "%20"))
				.userAgent("Mozilla/5.0 (Windows NT 6.2; WOW64; rv:16.0.1) Gecko/20121011 Firefox/16.0.1").ignoreHttpErrors(true)
				.ignoreContentType(true).referrer(url).timeout(0).execute();
		int statusCode = response.statusCode();
		if (statusCode == 200) {
			System.out.printf("%-10s\t%s\n", "fetched", url);
			doc = response.parse();
			Elements links = doc.select("a[href]");
			String linkStr = null;
			for (Element link : links) {
				linkStr = link.attr("abs:href");
				if (regex == null || linkStr.matches(regex)) {
					list.add(linkStr);
				}
			}
			return list;
		} else {
			System.err.printf("%-10s\t%s\n", statusCode, url);
			return null;
		}
	}

	public static String getSpecialText(String html, String incQuery, String[] excQuerys) {
		Document doc = Jsoup.parse(html);
		StringBuilder parsedContent = new StringBuilder();
		String temp = null;
		if (excQuerys != null) {
			for (Object rs : excQuerys) {
				doc.select(rs.toString()).remove();
			}
		}
		Elements elements = doc.select(incQuery);
		for (Element element : elements) {
			temp = getPlainText(element).trim();
			if (temp.length() > 0) {
				parsedContent.append(temp + "\n");
			}
		}
		return parsedContent.toString();
	}
	
	public static String getPlainText(Element element){
		FormattingVisitor formatter = new FormattingVisitor();
        NodeTraversor traversor = new NodeTraversor(formatter);
        traversor.traverse(element); // walk the DOM, and call .head() and .tail() for each node

        return formatter.toString();
	}
	
	private static class FormattingVisitor implements NodeVisitor {
        private StringBuilder accum = new StringBuilder(); // holds the accumulated text

        // hit when the node is first seen
        public void head(Node node, int depth) {
            String name = node.nodeName();
            if (node instanceof TextNode)
                append(((TextNode) node).text()); // TextNodes carry all user-readable text in the DOM.
            else if (name.equals("li"))
                append("\n* ");
            else if (name.equals("dd"))
                append("\t");
        }

        // hit when all of the node's children (if any) have been visited
        public void tail(Node node, int depth) {
            String name = node.nodeName();
            if (StringUtil.in(name, "br", "p", "h1", "h2", "h3", "h4", "h5", "ul", "ol", "dd", "dl", "dt", "pre"))
                append("\n");
        }

        // appends text to the string builder with a simple word wrap method
        private void append(String text) {
            if (text.equals(" ") &&
                    (accum.length() == 0 || StringUtil.in(accum.substring(accum.length() - 1), " ", "\n")))
                return; // don't accumulate long runs of empty spaces

                accum.append(text);
        }

        public String toString() {
            return accum.toString();
        }
    }
}
