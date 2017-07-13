import java.io.IOException;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import java.util.*;

public class contentMapper extends Mapper<Object, Text, Text, IntWritable> { 
	


private Text data = new Text();
private IntWritable numInstances = new IntWritable(1);
String country;

    @Override
	public void map(Object key, Text value, Context context) throws IOException, 		InterruptedException {
    countryNames countries = new countryNames();
	 String[][] countryArray = countryNames.countryNamesArray(); 

	String[] inputLines = value.toString().split(";");
	//Formating date to be able to group tweets by date
	try{	
		// Get the tweets
		// sort out the hashtags
		//compare the hashtags with the country array
		

			List<String> curHashtagsList = new ArrayList<String>();
	
	
			if (inputLines.length >= 3) {	
				
			String matchedCountry = "";
		    String curHash = inputLines[2];
			Pattern hashtagPattern = Pattern.compile("#(\\w+)");
			Matcher hashtagMatcher = hashtagPattern.matcher(curHash);
			while (hashtagMatcher.find()) {
					curHashtagsList.add(hashtagMatcher.group());      
			}
		
			int numCountries = countryArray.length;
			//itterates trought the hashatges of current tweet
			for(int a = 0;a < curHashtagsList.size();a++){
				//itterates trought the list of countries
				for(int b = 0;b < numCountries; b++){
					int curCountryLen = countryArray[b].length; //number of ways to write a country
					//itterates trought the different speellings of a country
					for(int c = 0;c < curCountryLen;c++){
						//itterates trough each hashtag to compare with country spelling
						if(curHashtagsList.get(a).toLowerCase().contains(countryArray[b][c].toLowerCase().replaceAll("\\s+",""))){
						//	matchedCountry = curHashtagsList.get(a);//prints out and counts hashtags
							matchedCountry = countryArray[b][0]; //prints out and counts countries
							data.set(matchedCountry);
							context.write(data,numInstances);
						}
					}
				}
			}
				
			
		 
	
			}
		
		
		
	
	
	}
	catch(Exception e){
		//e.printStackTrace();
		System.out.println("Error: " + e);
	}
	
        }
    }


