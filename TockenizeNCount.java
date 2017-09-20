package bigdata;

import java.net.*;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.TreeMap;
import java.io.*;
public class TockenizeNCount {

	public static void main(String[] args) throws Exception {


		        URL book = new URL("https://www.gutenberg.org/files/98/98-0.txt");
		        BufferedReader in = new BufferedReader(
		        new InputStreamReader(book.openStream()));
		    	File file = new File("C:\\Users\\HP\\Desktop\\sxk151632Part2.txt");
				FileOutputStream fileoutput = new FileOutputStream(file);
				if (!file.exists()) {
						file.createNewFile();
					}
					else
					{
						fileoutput.write("".getBytes());
					}
				HashMap<String,Integer> stringMap = new HashMap<String,Integer>();
		        String inputLine;
		        byte[] contentInBytes;
		        while ((inputLine = in.readLine()) != null)
		        {
		        	
		        	String[] stringArr = inputLine.split(" +");
		        	
		        	for(String s : stringArr)
		        		
		        	{	
		        		String  chrStr = s.replaceAll("[^\\w\\s]","");
		        		char[] chrArr = chrStr.toCharArray();
		        		if(chrArr.length>1 || ((chrArr.length==1)&&(chrArr[0]=='A' || chrArr[0]=='a')))
		        		{
		        			if(stringMap.containsKey(chrStr))
		        			{
		        				int count = stringMap.get(chrStr).intValue();
		        				stringMap.replace(chrStr, ++count);
		        			}
		        			else
		        			{
		        			stringMap.put(chrStr, 1);
		        			}
		        		}
		        	}
		        	
		        }
		        
		        List<Entry<String,Integer>> sortedWords = new ArrayList<Entry<String,Integer>>(stringMap.entrySet());

		        Collections.sort(sortedWords,new Comparator<Entry<String,Integer>>() {
		        				public int compare(Entry<String,Integer> entry1, Entry<String,Integer> entry2) {
		                        return entry2.getValue().compareTo(entry1.getValue());
		                    }
		                }
		        );
		        Iterator iterator = sortedWords.iterator();
		        while(iterator.hasNext())
	        	{
		        	contentInBytes = iterator.next().toString().getBytes();
		        	fileoutput.write(contentInBytes);
		        	fileoutput.write("\n".getBytes());
	        	}
		        in.close();
		        fileoutput.flush();
		        fileoutput.close();
		    }
}

