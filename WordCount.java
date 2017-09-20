import java.io.BufferedReader;
import java.io.FileReader;
import java.io.FileWriter;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.PrintWriter;
import java.net.MalformedURLException;
import java.net.URL;

import java.nio.channels.Channels;
import java.nio.channels.ReadableByteChannel;
 
public class WordCount {
	
	public static void main(String[] args) throws Exception {
		String url = "http://www.gutenberg.org/files/98/98-0.txt";
		String file = "file.txt";
		ReadableByteChannel readableBC = null;
		FileOutputStream fileOS = null;
		try {
			URL urlObj = new URL(url);
			readableBC = Channels.newChannel(urlObj.openStream());
			fileOS = new FileOutputStream(file);
			fileOS.getChannel().transferFrom(readableBC, 0, Long.MAX_VALUE);
		} catch (MalformedURLException e) {
			e.printStackTrace();
		} catch (IOException e) {
			e.printStackTrace();
		}finally{
			try {
				if(fileOS != null)
					fileOS.close();
			} catch (IOException e) {
				e.printStackTrace();
			}
			try {
				if(readableBC != null)
					readableBC.close();
			} catch (IOException e) {
				e.printStackTrace();
			}
		}
		Path path = Paths.get("./").resolve("file.txt");
		BufferedReader reader = new BufferedReader(new FileReader(path.toFile()));
		Map<String, Integer> frequency = new HashMap<>();
		String line = reader.readLine();
		while(line != null) {
			if(!line.trim().equals("")) {
				String [] words = line.split(" ");
				for(String word : words) {
					if(word == null || word.trim().equals(""))
						continue;
					String processed = word.toLowerCase();
					processed = processed.replace(",", "");
					if(frequency.containsKey(processed)) {
						frequency.put(processed, frequency.get(processed) + 1);
					} else {
						frequency.put(processed, 1);
					}
				}
			}
			line = reader.readLine();
		}
		Set<Entry<String, Integer>> set = frequency.entrySet();
		ArrayList<Entry<String, Integer>> list = new ArrayList<Entry<String, Integer>>(set);
		Collections.sort(list, new Comparator<Map.Entry<String, Integer>>() {
			public int compare(Map.Entry<String, Integer> o1, Map.Entry<String, Integer> o2) {
				int result = (o2.getValue()).compareTo( o1.getValue() );
				if (result != 0) {
					return result;
				} else {
					return o1.getKey().compareTo(o2.getKey());
				}
			}
		});
		PrintWriter out = null;
		try {
			out = new PrintWriter(new FileWriter("vxa162530Part2.txt"));
			for (Entry<String, Integer> entry: list) {
				out.println(entry);
			}
			out.close();
			System.out.println("The output file has been generated successfully");
		} catch (IOException e) {
			e.printStackTrace();
		}
	}
}