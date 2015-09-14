package audit.hdfs;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;

public class ParseTool extends Thread{
    private String srcDirName;
    private String desName;
    private String oprType;
    private String userFlag;
    
    private HashMap<String, ArrayList<String>> statDatas;
    private UserStat userStat;
    private QpsStat qpsStat;
    private File file;
    
    public ParseTool(String srcDirName, String desName, String oprType){
    	this.srcDirName = srcDirName;
    	this.desName = desName;
    	this.oprType = oprType;
    	this.userFlag = null;
    	
    	this.statDatas = new HashMap<String, ArrayList<String>>();
    	this.userStat = new UserStat(oprType);
    }
    
    public ParseTool(String srcDirName, String desName, String oprType, int timeInternal){
    	this.srcDirName = srcDirName;
    	this.desName = desName;
    	this.oprType = oprType;
    	this.userFlag = null;
    	
    	this.statDatas = new HashMap<String, ArrayList<String>>();
    	this.qpsStat = new QpsStat(timeInternal);
    }
    
    public ParseTool(String srcDirName, String desName, String oprType, String userFlag){
    	this.srcDirName = srcDirName;
    	this.desName = desName;
    	this.oprType = oprType;
    	this.userFlag = userFlag;
    	
    	this.statDatas = new HashMap<String, ArrayList<String>>();
    	this.userStat = new UserStat(oprType);
    }
    
    private void readDataDir(){
    	File dir = new File(srcDirName);
    	File desFile;
    	File[] fileList = dir.listFiles();
        String namePrefix;
        
        desFile = null;
        namePrefix = "hdfs-audit.log.";
    	for(int i=100; i>=1; i--){
    		for(File f: fileList){
    			if(f.getName().equals(namePrefix + i)){
    				desFile = f;
    				break;
    			}
    		}
    		
    		if(desFile != null){
                System.out.println("selected file is " + desFile.getName());
        		
        		readDataFile(desFile);
        	}

    		if(i == 1){
        	    System.out.println("selected file is used out");
        		
        		if(this.oprType.equals("username") || this.oprType.equals("ip")){
        			userStat.printFinishedResult();
        		}
    		}
    		
    		desFile = null;
    	}
    }
    
    /**
	 * 从文件中读取数据
	 */
	private void readDataFile(File file) {
		//System.out.println("****** begining *****");
		
		try {
			BufferedReader in = new BufferedReader(new FileReader(file));
			String str;
			if(file != null){
		//		System.out.println("file name is " + file.getName());
			}else{
		//		System.out.println("file is null");
			}
			
			while ((str = in.readLine()) != null) {
				if(this.oprType.equals("username") || this.oprType.equals("ip")){
					if(userFlag == null || userFlag.equals("")){
						userStat.parseUserRequets(str);
					}else{
						userStat.parseSingleUserRequets(str, userFlag);
					}
				}else if(this.oprType.equals("qps")){
					qpsStat.parseQps(str);
				}
			}
			in.close();
		} catch (IOException e) {
			e.getStackTrace();
		}
		
		//System.out.println("****** ending *****");
	}

	@Override
	public void run() {
		// TODO Auto-generated method stub
		super.run();
		
		readDataDir();
	}
	
	private void parsrIpRequests(String record){
		int pos;
		String ip;
		String opr;
		String tmp;
		String[] array;
		ArrayList<String> cmds;
		
		pos = record.indexOf("ip");
		
		if(pos == -1){
			System.out.println(record + " is invalid.");
			return;
		}
		
		tmp = record.substring(pos);
		array = tmp.split("\t");
		ip = array[0].split("=")[1];
		
		if(array[1].equals("")){
			opr = array[2].split("=")[1];	
		}else{
			opr = array[1].split("=")[1];
		}
		
		if(this.statDatas.containsKey(ip)){
			cmds = this.statDatas.get(ip);
		}else{
			cmds = new ArrayList<String>();
		}
		cmds.add(opr);
		
		this.statDatas.put(ip, cmds);
	}
	
	private void printResult(){
		String ip;
		
		for(Map.Entry<String, ArrayList<String>> entry: this.statDatas.entrySet()){
			ip = entry.getKey();
			System.out.println(ip + " operations total num is " + entry.getValue().size());
		}
	}
}
