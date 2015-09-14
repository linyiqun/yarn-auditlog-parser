package audit.hdfs;

import java.io.File;

public class Main {
   public static void main(String[] args){
	   int timeInternal;
	   String userFlag;
	   String fileDirName;
	   String oprType;
	   
	   ParseTool tool;
	   
	   timeInternal = 1;
	   fileDirName = "//Users//lyq//Desktop/testFile2";
	   oprType = "ip";
	   userFlag = "192.168.10.10";
	   
	   tool = null;
	   if(args.length == 2){
		   fileDirName = args[0];
		   System.out.println("fileDirName is" + fileDirName);
		   oprType = args[1];
		   tool = new ParseTool(fileDirName, "", oprType);
	   }else if (args.length == 3){
		   fileDirName = args[0];
		   oprType = args[1];
		   
		   if(oprType.equals("qps")){
			   timeInternal = Integer.parseInt(args[2]);
			   tool = new ParseTool(fileDirName, "", oprType, timeInternal);
			   
			   System.out.println("fileDirName is" + fileDirName);
			   System.out.println("timeInternal is" + timeInternal);
		   }else if (oprType.equals("ip")){
			   tool = new ParseTool(fileDirName, "", oprType, "/" + args[2]);
			   System.out.println("the target user is " + args[2]);
		   }else if (oprType.equals("username")){
			   tool = new ParseTool(fileDirName, "", oprType, args[2] + " (auth:SIMPLE)");
			   System.out.println("the target user is " + args[2]);
		   }
	   }else{
		   tool = new ParseTool(fileDirName, "", oprType, "/" + args[2]);
	   }
	   
	   File srcFile = new File(fileDirName);
		if (!srcFile.isDirectory()) {
			System.out.println(fileDirName + " is not directory");
			System.exit(1);
		}
	   
	   tool.run();
   }
}
