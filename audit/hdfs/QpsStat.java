package audit.hdfs;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

public class QpsStat {
	int currentMinute;
    int tmpCount;
    int totalOpr;
    int timeInternal;
    
    String currentHour;
    
    HashMap<String, HashMap<String, Integer>> statDatas;
   
   
    public QpsStat(int timeInternal){
	   this.totalOpr = 0;
	   this.tmpCount = 0;
	   this.currentMinute = -1;
	   this.timeInternal = timeInternal;
	   
	   this.currentHour = "";
	   this.statDatas = new HashMap<String, HashMap<String,Integer>>();
    }   
   
    public void parseQps(String record){
	    int pos;
	    int MinuteEnd;
	    int minute;
		int count;
		
		String user;
		String timeStr;
		String tmp;
		String[] array;
		HashMap<String, Integer> data;
		
		pos = record.indexOf("ugi");
		MinuteEnd = record.indexOf(",");
		
		if(pos == -1){
			System.out.println(record + "not found");
			return;
		}
		
		tmp = record.substring(pos);
		array = tmp.split("\t");
		user = array[0].split("=")[1];
		
		tmp = record.substring(0, MinuteEnd);
		tmp = tmp.substring(0, tmp.lastIndexOf(":"));
		MinuteEnd = tmp.lastIndexOf(":");
		tmp = tmp.substring(tmp.lastIndexOf(":")+1);		
		minute = Integer.parseInt(tmp);
		
		timeStr = record.substring(0, record.indexOf(":"));
		if(!timeStr.equals(currentHour)){
 			tmpCount = 0;
 			
 			if(timeStr.equals("")){
 				currentMinute = minute;
 			}else{
 				currentMinute = 0;
 			}
 			
			currentHour = timeStr;
		}
		
		if(currentMinute == -1){
			this.currentMinute = minute;
		}
		
		if((currentMinute + timeInternal) > minute && (currentMinute + timeInternal) < 60){
		    tmpCount++;	
		    countUser(currentHour + ":" + currentMinute, user);
		}else if((currentMinute + timeInternal) == 60 && minute > 10){
			tmpCount++;
			countUser(currentHour + ":" + currentMinute, user);
		}else{
			//printHourResult();
			this.statDatas.clear();
			System.out.println("start time " + currentHour + ":" + currentMinute + ", in " + timeInternal + " minutes , qps num is " + tmpCount);
			
			tmpCount = 0;
		    currentMinute = minute;
		}
   }
    
    private void countUser(String time, String user){
    	int count;
    	HashMap<String, Integer> userDatas;
    	
    	if(this.statDatas.containsKey(time)){
    		userDatas = this.statDatas.get(time);
    	}else{
    		userDatas = new HashMap<String, Integer>();
    	}
    	
    	if(userDatas.containsKey(user)){
    		count = userDatas.get(user);
    	}else{
    		count = 0;
    	}
    	count++;
    	userDatas.put(user, count++);
    	
    	this.statDatas.put(time, userDatas);
    }
    
    public void printHourResult(){
		String time;
		int count;
		int totalCount;
		
		HashMap<String, Integer> mapData;
		ArrayList<HourUserData> list;
		
		totalCount = 0;
		
		for(Map.Entry<String, HashMap<String, Integer>> entry: this.statDatas.entrySet()){
			time = entry.getKey();
			System.out.println("the time is " + time);
			
			mapData = entry.getValue();
			list = new ArrayList<HourUserData>();
			for(Map.Entry<String, Integer> e: mapData.entrySet()){
				list.add(new HourUserData(e.getKey(), e.getValue()));
			}
			
			Collections.sort(list);
			for(HourUserData hud: list){
				hud.printInfo();
			}
		}
	}
}
