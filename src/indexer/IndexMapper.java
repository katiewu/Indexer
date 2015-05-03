package indexer;

import java.io.IOException;
import java.math.BigInteger;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.Formatter;
import java.util.HashMap;
import java.util.HashSet;
import java.util.StringTokenizer;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.jsoup.Jsoup;
import org.jsoup.nodes.Document;
import org.jsoup.nodes.Element;
import org.jsoup.select.Elements;

import snowballstemmer.PorterStemmer;

public class IndexMapper extends Mapper<Text, Text, Text, Text> {

	private static final String DELIMATOR = " \t\n\r\"'-_/.,:;|{}[]!@#%^&*()<>=+`~?";
	private static final double a = 0.4;
	
	private static HashSet<String> stopWords = new HashSet<String>();

	static {
		String[] lists = { "edu", "com", "html", "htm", "xml", "php", "org",
				"gov", "net", "int", "jpg", "png", "bmp", "jpeg", "pdf", "asp",
				"aspx" };
		for (String word : lists) {
			stopWords.add(word);
		}
	}
	
	public static String html2text(String content) {
	    return Jsoup.parse(content).text();
	}
	
	public static String stemContent(String content) {
		StringTokenizer tokenizer = new StringTokenizer(content, DELIMATOR);
		String word = "";
		PorterStemmer stemmer = new PorterStemmer();
		StringBuilder sb = new StringBuilder();
		while (tokenizer.hasMoreTokens()) {
			word = tokenizer.nextToken();
			if(word.equals("")) continue;
			boolean flag = false;
			for(int i=0;i<word.length();i++){
				if (Character.UnicodeBlock.of(word.charAt(i)) != Character.UnicodeBlock.BASIC_LATIN) {
					flag = true;
					break;
				}
			}	
			if(flag) continue;
			stemmer.setCurrent(word);
			if(stemmer.stem()){
				sb.append(stemmer.getCurrent());
				sb.append(" ");
			}
		}
		return new String(sb);
	}
	
	public static String toBigInteger(String key) {
		try {
			MessageDigest messageDigest = MessageDigest.getInstance("SHA-1");
			messageDigest.update(key.getBytes());
			byte[] bytes = messageDigest.digest();
			Formatter formatter = new Formatter();
			for (int i = 0; i < bytes.length; i++) {
				formatter.format("%02x", bytes[i]);
			}
			String resString = formatter.toString();
			formatter.close();
			return String.valueOf(new BigInteger(resString, 16));
		} catch (NoSuchAlgorithmException e) {
			e.printStackTrace();
		}
		return String.valueOf(new BigInteger("0", 16));
	}
	
	public static boolean isNumber(String s) {
		String pattern = "\\d+";
		// Create a Pattern object
		Pattern r = Pattern.compile(pattern);
		// Now create matcher object.
		Matcher m = r.matcher(s);
		return m.find();
	}
	
	public void analURL(String url, String docID, Context context) throws IOException, InterruptedException {
		if(url.startsWith("http://")){
			url = url.substring(7);
		}
		else if(url.startsWith("https://")){
			url = url.substring(8);
		}
		if(url.startsWith("www.")){
			url = url.substring(4);
		}
		url = stemContent(url);
		StringTokenizer tokenizer = new StringTokenizer(url, DELIMATOR);
		String word = "";
		while (tokenizer.hasMoreTokens()) {
			word = tokenizer.nextToken();
			if(word.equals("") || word.length()>20) continue;
			if(isNumber(word)) continue;
			if(!stopWords.contains(word)){
				String result = "1\t"+docID;
				context.write(new Text(word), new Text(result));
			}
		}
	}
	
	public static void splitKey(String content, String docID, int type, Context context){
		String store_text = stemContent(content);
		StringTokenizer tokenizer = new StringTokenizer(store_text, DELIMATOR);
		String word = "";
		while (tokenizer.hasMoreTokens()) {
			word = tokenizer.nextToken();
			if(word.equals("")) continue;
			boolean flag = false;
			for(int i=0;i<word.length();i++){
				if (Character.UnicodeBlock.of(word.charAt(i)) != Character.UnicodeBlock.BASIC_LATIN) {
					flag = true;
					break;
				}
			}	
			if(flag) continue;
			// write the word and url to file
			Text key = new Text(word);
			Text value = new Text(docID+"\t-1\t,\t"+type);
			try {
				context.write(key, value);
			} catch (IOException | InterruptedException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		}
	}
	
	public void map(Text key, Text value, Context context) throws IOException, InterruptedException {
		// key: docID, value: content
		String docID = toBigInteger(key.toString());
		// get raw content
		String url = key.toString();
		String rawcontent = value.toString();
		
		// type: 0:raw_data, 1:url, 2:meta, 3:anchor, 4:title
		
		// type 0: analyze the raw data
		String content = html2text(rawcontent.toLowerCase());
		// stemming the content
		content = stemContent(content);
		
		// process one document
		HashMap<String, WordInfo> wordSet = new HashMap<String, WordInfo>();
		int position = 0;
		StringTokenizer tokenizer = new StringTokenizer(content, DELIMATOR);
		String word = "";
		int max = 0;
		while (tokenizer.hasMoreTokens()) {
			word = tokenizer.nextToken();
			if(word.equals("")) continue;
			if(!wordSet.containsKey(word)){
				WordInfo info = new WordInfo();
				wordSet.put(word, info);
			}
			wordSet.get(word).addPosition(position);
			int size = wordSet.get(word).getSize();
			if(size>max) max = size;
			position++;
		}
		for(String w:wordSet.keySet()){
			double tf = a + (1-a)*wordSet.get(w).getSize()/max;
			wordSet.get(w).setTF(tf);
			WordInfo wi = wordSet.get(w);
			String wordinfo = docID+"\t"+tf+"\t"+wi.positionList+"\t0";
			context.write(new Text(w), new Text(wordinfo));
		}
		
		//type > 0
		analURL(url, docID, context);
		Document doc = Jsoup.parse(rawcontent, url);
		// extract anchor text
		Elements links = doc.select("a");
		for (Element link : links) {
			String abshref = link.attr("abs:href");
			String relhref = link.attr("href");
			String title = link.attr("title").toLowerCase();
			String text = link.text().toLowerCase();
			if (abshref.equals("") || relhref.startsWith("#")) {
				// if there is no reference, or reference starts with #, replies
				// jump to fragment
				continue;
			}
			if(abshref.contains("#")){
				int index = abshref.indexOf("#");
				abshref = abshref.substring(0, index);
			}
			String linkID = toBigInteger(abshref);
			splitKey(text + " " + title, linkID, 3, context);
		}
		// extract meta data
		Elements metas = doc.select("meta");
		for(Element meta:metas){
			String metacontent = meta.attr("content").toLowerCase();
			splitKey(metacontent, docID, 2, context);
		}
		// extract title
		Elements titles = doc.select("title");
		for (Element title : titles) {
			String titlecontent = title.text().toLowerCase();
			if (titlecontent.equals(""))
				continue;
			splitKey(titlecontent, docID, 4, context);
		}
	}

}


class WordInfo{
	int size;
	String positionList;
	double tf;
	public WordInfo(){
		this.size = 0;
		positionList = "";
		tf = 0;
	}
	
	public int getSize(){
		return size;
	}
	
	public String getPositions(){
		return positionList;
	}
	
	public double getTF(){
		return tf;
	}
	
	public void addPosition(int position){
		if(!positionList.equals("")) positionList += ",";
		positionList += position;
		size++;
	}
	
	public void setTF(double tf){
		this.tf = tf;
	}
}
