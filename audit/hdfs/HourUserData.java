package audit.hdfs;

public class HourUserData implements Comparable<HourUserData>{
    String userName;
    Integer count;
    
    public HourUserData(String userName, Integer count){
    	this.userName = userName;
    	this.count = count;
    }

	@Override
	public int compareTo(HourUserData o) {
		// TODO Auto-generated method stub
		return o.count.compareTo(this.count);
	}
	
	public void printInfo(){
		System.out.println(userName + " opration num is " + count);
	}
}
