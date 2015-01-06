
//This class stores the mean, median, mode, and standard deviation 
public class results {

	String name;
	int interval;
	long mean;
	int median;
	int mode;
	long sd;
	
	public results(){
		name = "";
		interval = 0;
		mean = 0;
		median = 0;
		mode = 0;
		sd = 0;
	}
	
	public results(String passed_name, int passed_interval, long passed_mean, int passed_median, int passed_mode, long passed_sd){
		name = passed_name;
		interval = passed_interval * 5;
		mean = passed_mean;
		median = passed_median;
		mode = passed_mode;
		sd = passed_sd;
	}
	
	public void setName(String passed_name){
		name = passed_name;
	}
	
	public void setInterval(int passed_interval){
		interval = passed_interval;
	}
	
	public void setMean(long passed_mean){
		mean = passed_mean;
	}
	
	public void setMedian(int passed_median){
		median = passed_median;
	}
	
	public void setMode(int passed_mode){
		mode = passed_mode;
	}
	
	public void setSD(long passed_sd){
		sd = passed_sd;
	}

	public String toString(){
		String line = "name: " + name + ", interval: " + interval + ", mean: " + mean + ", median: " + median + ", mode: " + mode + ", stand deviation: " + sd;
		return line;
	}
}
