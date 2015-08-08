import java.io.BufferedReader;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.HashMap;
import java.util.regex.Pattern;



public class CountTweetID {

	public static void main(String[] args) {
		String path = "/Users/hiroki/Desktop/MIJS_Data/tweets.100000.csv";
		fileRead(path);
	}

	public static void fileRead(String filePath) {
		HashMap<String, Integer> idMap = new HashMap<String, Integer>();
		FileInputStream fis = null; 
		InputStreamReader in = null; 
		BufferedReader inFile = null;
    	String regex = "^[0-9]*,";
        Pattern p = Pattern.compile(regex);

	    try {
	    	fis = new FileInputStream(filePath); 
	    	//in = new InputStreamReader(fis,"UTF-8"); 
	    	in = new InputStreamReader(fis,"Shift-JIS"); 
	    	inFile = new BufferedReader(in);
	 
	        String line;
	        while ((line = inFile.readLine()) != null) {
	        	// Tweet行でない
	            if (!p.matcher(line).find()) continue;

	            int idx = line.indexOf(",");
	        	if (idx != -1) {
		        	String tmp = line.substring(0, idx);
		        	// すでにIDが含まれる
		        	if (idMap.containsKey(tmp)) {
		        		idMap.put(tmp, idMap.get(tmp) + 1);
		        	} else {
		        		idMap.put(tmp, 1);
		        	}
	        	}
	        }
            for (String key : idMap.keySet()) {
            	System.out.println(key + "," + idMap.get(key));
            } 
	    } catch (FileNotFoundException e) {
	        e.printStackTrace();
	    } catch (IOException e) {
	        e.printStackTrace();
	    } finally {
	        try {
	        	inFile.close();
	        	in.close();
	        	fis.close();
	        } catch (IOException e) {
	            e.printStackTrace();
	        }
	    }
	}
}
