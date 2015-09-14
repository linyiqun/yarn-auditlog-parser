package audit.hdfs;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

public class UserStat implements Comparable<UserStat>{
	String userType;
	String currentHour;
	
    Integer totalOprNum;
    HashMap<String, HashMap<String, Integer>> statDatas;
    HashMap<String, Integer> sumData;
    
    public UserStat(String userType) {
		this.totalOprNum = 0;
		this.currentHour = "";
		this.userType = userType;
		
		this.statDatas = new HashMap<String, HashMap<String, Integer>>();
		this.sumData = new HashMap<String, Integer>();
	}
    
    public void parseUserRequets(String record){
		int pos;
		int count;
		
		String user;
		String timeStr;
		String tmp;
		String[] array;
		HashMap<String, Integer> data;
		
		
		pos = 0;
		user = "";
		if(userType.equals("ip")){
			pos = record.indexOf("ip");
			if(pos == -1){
				System.out.println(record + " is invalid.");
				return;
			}
			
			tmp = record.substring(pos);
			array = tmp.split("\t");
			user = array[0].split("=")[1];
		}else if(userType.equals("username")){
			pos = record.indexOf("ugi");
			if(pos == -1){
				System.out.println(record + " is invalid.");
				return;
			}
			
			tmp = record.substring(pos);
			
			array = tmp.split("\t");
			user = array[0].split("=")[1];
		}
        
		
		//opr = array[2].split("=")[1];
		
		timeStr = record.substring(0, record.indexOf(":"));
		if(!timeStr.equals(currentHour)){
			currentHour = timeStr;
			//System.out.println("new time hour" + timeStr);
			printHourResult();
			this.statDatas.clear();
		}
		
		if(this.statDatas.containsKey(timeStr)){
			data = statDatas.get(timeStr);
		}else{
			data = new HashMap<String, Integer>();
		}
		
		if(data.containsKey(user)){
			count = data.get(user);
		}else{
			count = 0;
		}
		count++;
		
		data.put(user, count);
		this.statDatas.put(timeStr, data);
	}
    
    public void parseSingleUserRequets(String record, String userFlag){
		int pos;
		int count;
		
		String cmd;
		String user;
		String timeStr;
		String tmp;
		String[] array;
		HashMap<String, Integer> data;
		
		
		pos = 0;
		user = "";
		array = null;
		if(userType.equals("ip")){
			pos = record.indexOf("ip");
			if(pos == -1){
				System.out.println(record + " is invalid.");
				return;
			}
			
			tmp = record.substring(pos);
			array = tmp.split("\t");
			user = array[0].split("=")[1];
		}else if(userType.equals("username")){
			pos = record.indexOf("ugi");
			if(pos == -1){
				System.out.println(record + " is invalid.");
				return;
			}
			
			tmp = record.substring(pos);
			
			array = tmp.split("\t");
			user = array[0].split("=")[1];
		}
        
		if(!user.equals(userFlag)){
			return;
		}
		
		if(userType.equals("username")){
			cmd = array[2].split("=")[1];
		}else{
			cmd = array[1].split("=")[1];
		}
		
		
		timeStr = record.substring(0, record.indexOf(":"));
		if(!timeStr.equals(currentHour)){
			currentHour = timeStr;
			//System.out.println("new time hour" + timeStr);
			printHourResult();
			this.statDatas.clear();
		}
		
		if(this.statDatas.containsKey(timeStr)){
			data = statDatas.get(timeStr);
		}else{
			data = new HashMap<String, Integer>();
		}
		
		if(data.containsKey(cmd)){
			count = data.get(cmd);
		}else{
			count = 0;
		}
		count++;
		
		data.put(cmd, count);
		this.statDatas.put(timeStr, data);
	}
    
	@Override
	public int compareTo(UserStat o) {
		// TODO Auto-generated method stub
		return totalOprNum.compareTo(o.totalOprNum);
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
			System.out.println("the time hour is " + time);
			
			mapData = entry.getValue();
			list = new ArrayList<HourUserData>();
			for(Map.Entry<String, Integer> e: mapData.entrySet()){
				list.add(new HourUserData(e.getKey(), e.getValue()));
			}
			
			Collections.sort(list);
			for(HourUserData hud: list){
				hud.printInfo();
				
				if(sumData.containsKey(hud.userName)){
					count = sumData.get(hud.userName);
				}else{
					count = 0;
				}
				count += hud.count;
				totalCount += hud.count;
				
				sumData.put(hud.userName, count);
			}
			
			System.out.println("the time hour " + time + " total operation is " + totalCount + ", qps is " + (1.0 * totalCount/3600));
		}
	}
	
	public void printFinishedResult(){
		ArrayList<HourUserData> list;
		int totalNum;
		
		totalNum = 0;
		//输出总的统计
		list = new ArrayList<HourUserData>();
		for(Map.Entry<String, Integer> e: sumData.entrySet()){
			list.add(new HourUserData(e.getKey(), e.getValue()));
			totalNum += e.getValue();
		}
		
		System.out.println("汇总统计结果:");
		Collections.sort(list);
		for(HourUserData hud: list){
			hud.printInfo();
	    }
		System.out.println("total operation is " + totalNum);
	}
}
