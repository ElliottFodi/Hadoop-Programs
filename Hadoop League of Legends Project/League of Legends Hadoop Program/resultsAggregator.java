import java.util.ArrayList;


//this class stores the results of all the stats collected
//the results are stored in a results class 
public class resultsAggregator {
	
	//intervals == to the amount of hash maps in each array 
	int intervals = 0; 
	
	//general stats 
	ArrayList<results> gold = new ArrayList<results>();
	ArrayList<results> avgTeamLevel = new ArrayList<results>();
	ArrayList<results> totalTeamXP = new ArrayList<results>();
	ArrayList<results> minionsKilled = new ArrayList<results>();
	ArrayList<results> jungleMinionsKilled = new ArrayList<results>();
	ArrayList<results> championsKilled = new ArrayList<results>();
	ArrayList<results> wardsPlaced = new ArrayList<results>();
	ArrayList<results> healthPotionsBought = new ArrayList<results>();
	ArrayList<results> manaPotionsBought = new ArrayList<results>();
	ArrayList<results> visionWardsBought = new ArrayList<results>();
	ArrayList<results> stealthWardsBought = new ArrayList<results>();
	
	// elite monsters
	ArrayList<results> RED_LIZARD = new ArrayList<results>();
	ArrayList<results> BLUE_GOLEM = new ArrayList<results>();
	ArrayList<results> DRAGON = new ArrayList<results>();
	ArrayList<results> BARON_NASHOR = new ArrayList<results>();
	
	// buildings
	ArrayList<results> inhibitor = new ArrayList<results>();
	ArrayList<results> tower = new ArrayList<results>();

	// should have buildings
	ArrayList<results> base_turret = new ArrayList<results>();
	ArrayList<results> fountain_turret = new ArrayList<results>();
	ArrayList<results> inner_turret = new ArrayList<results>();
	ArrayList<results> nexus_turret = new ArrayList<results>();
	ArrayList<results> outer_turret = new ArrayList<results>();
	ArrayList<results> undefined_turret = new ArrayList<results>();
	
	//Constructor
	public resultsAggregator(int passed_interval) {
		intervals = passed_interval;

		// add results objects to each array based on the interval being sampled
		for (int i = 0; i < passed_interval; i++) {
			gold.add(new results());
			avgTeamLevel.add(new results());
			totalTeamXP.add(new results());
			minionsKilled.add(new results());
			jungleMinionsKilled.add(new results());
			championsKilled.add(new results());
			wardsPlaced.add(new results());
			healthPotionsBought.add(new results());
			manaPotionsBought.add(new results());
			visionWardsBought.add(new results());
			stealthWardsBought.add(new results());

			// elite monsters
			RED_LIZARD.add(new results());
			BLUE_GOLEM.add(new results());
			DRAGON.add(new results());
			BARON_NASHOR.add(new results());

			// building types
			inhibitor.add(new results());
			tower.add(new results());

			// tower types
			base_turret.add(new results());
			fountain_turret.add(new results());
			inner_turret.add(new results());
			nexus_turret.add(new results());
			outer_turret.add(new results());
			undefined_turret.add(new results());
		}
	}
	
	//set the results object at an interval in an array 
	public void setResult(String passed_arrayName, int passed_interval,
			long passed_mean, int passed_median, int passed_mode, long passed_sd) {

		switch (passed_arrayName) {

		case "gold":
			setResultObject(gold, passed_arrayName, passed_interval, passed_mean, 
					passed_median, passed_mode, passed_sd);
			break;

		case "avgTeamLevel":
			setResultObject(avgTeamLevel, passed_arrayName, passed_interval, passed_mean, 
					passed_median, passed_mode, passed_sd);
			break;

		case "totalTeamXP":
			setResultObject(totalTeamXP, passed_arrayName, passed_interval, passed_mean, 
					passed_median, passed_mode, passed_sd);
			break;

		case "minionsKilled":
			setResultObject(minionsKilled, passed_arrayName, passed_interval, passed_mean, 
					passed_median, passed_mode, passed_sd);
			break;

		case "jungleMinionsKilled":
			setResultObject(jungleMinionsKilled, passed_arrayName, passed_interval, passed_mean, 
					passed_median, passed_mode, passed_sd);
			break;

		case "championsKilled":
			setResultObject(championsKilled, passed_arrayName, passed_interval, passed_mean, 
					passed_median, passed_mode, passed_sd);
			break;

		case "wardsPlaced":
			setResultObject(wardsPlaced, passed_arrayName, passed_interval, passed_mean, 
					passed_median, passed_mode, passed_sd);
			break;

		case "healthPotionsBought":
			setResultObject(healthPotionsBought, passed_arrayName, passed_interval, passed_mean, 
					passed_median, passed_mode, passed_sd);
			break;

		case "manaPotionsBought":
			setResultObject(manaPotionsBought, passed_arrayName, passed_interval, passed_mean, 
					passed_median, passed_mode, passed_sd);
			break;

		case "visionWardsBought":
			setResultObject(visionWardsBought, passed_arrayName, passed_interval, passed_mean, 
					passed_median, passed_mode, passed_sd);
			break;

		case "stealthWardsBought":
			setResultObject(stealthWardsBought, passed_arrayName, passed_interval, passed_mean, 
					passed_median, passed_mode, passed_sd);
			break;

		case "RED_LIZARD":
			setResultObject(RED_LIZARD, passed_arrayName, passed_interval, passed_mean, 
					passed_median, passed_mode, passed_sd);
			break;

		case "BLUE_GOLEM":
			setResultObject(BLUE_GOLEM, passed_arrayName, passed_interval, passed_mean, 
					passed_median, passed_mode, passed_sd);
			break;

		case "DRAGON":
			setResultObject(DRAGON, passed_arrayName, passed_interval, passed_mean, 
					passed_median, passed_mode, passed_sd);
			break;

		case "BARON_NASHOR":
			setResultObject(BARON_NASHOR, passed_arrayName, passed_interval, passed_mean, 
					passed_median, passed_mode, passed_sd);
			break;

		case "INHIBITOR_BUILDING":
			setResultObject(inhibitor, passed_arrayName, passed_interval, passed_mean, 
					passed_median, passed_mode, passed_sd);
			break;

		case "TOWER":
			setResultObject(tower, passed_arrayName, passed_interval, passed_mean, 
					passed_median, passed_mode, passed_sd);
			break;

		case "BASE_TURRET":
			setResultObject(base_turret, passed_arrayName, passed_interval, passed_mean, 
					passed_median, passed_mode, passed_sd);
			break;

		case "FOUNTAIN_TURRET":
			setResultObject(fountain_turret, passed_arrayName, passed_interval, passed_mean, 
					passed_median, passed_mode, passed_sd);
			break;

		case "INNER_TURRET":
			setResultObject(inner_turret, passed_arrayName, passed_interval, passed_mean, 
					passed_median, passed_mode, passed_sd);
			break;

		case "NEXUS_TURRET":
			setResultObject(nexus_turret, passed_arrayName, passed_interval, passed_mean, 
					passed_median, passed_mode, passed_sd);
			break;

		case "OUTER_TURRET":
			setResultObject(outer_turret, passed_arrayName, passed_interval, passed_mean, 
					passed_median, passed_mode, passed_sd);
			break;

		case "UNDEFINED_TURRET":
			setResultObject(undefined_turret, passed_arrayName, passed_interval, passed_mean, 
					passed_median, passed_mode, passed_sd);
			break;

		default:
			// do nothing invalid input
			break;

		}

	}
	
	//sets the resultes object to the passed values at an interval in the specified array
	public static void setResultObject(ArrayList<results> passed_array, String passed_name, 
			int passed_interval, long passed_mean, int passed_median, int passed_mode, long passed_sd){
		passed_array.get(passed_interval).setName(passed_name);
		passed_array.get(passed_interval).setInterval(passed_interval);
		passed_array.get(passed_interval).setMean(passed_mean);
		passed_array.get(passed_interval).setMedian(passed_median);
		passed_array.get(passed_interval).setMode(passed_mode);
		passed_array.get(passed_interval).setSD(passed_sd);
	}
	
	//custom toString
	//the reducer calls this when writing its output to HDFS
	public String toString(){
		String line = "";
		line += getStringForArray(gold) + "\n";
		line += getStringForArray(avgTeamLevel)  + "\n";
		line += getStringForArray(totalTeamXP) + "\n";
		line += getStringForArray(minionsKilled) + "\n";
		line += getStringForArray(jungleMinionsKilled) + "\n";
		line += getStringForArray(championsKilled) + "\n";
		line += getStringForArray(wardsPlaced) + "\n";
		line += getStringForArray(healthPotionsBought) + "\n";
		line += getStringForArray(manaPotionsBought) + "\n";
		line += getStringForArray(visionWardsBought) + "\n";
		line += getStringForArray(stealthWardsBought) + "\n";
				// elite monsters
		line += getStringForArray(RED_LIZARD) + "\n";
		line += getStringForArray(BLUE_GOLEM) + "\n";
		line += getStringForArray(DRAGON) + "\n";
		line += getStringForArray(BARON_NASHOR) + "\n";
				// buildings
		line += getStringForArray(inhibitor) + "\n";
		line += getStringForArray(tower) + "\n";

				// should have buildings
		line += getStringForArray(base_turret) + "\n";
		line += getStringForArray(fountain_turret) + "\n";
		line += getStringForArray(inner_turret) + "\n";
		line += getStringForArray(nexus_turret) + "\n";
		line += getStringForArray(outer_turret) + "\n";
		line += getStringForArray(undefined_turret) + "\n";
		
		return line;
	}
	
	//calls the toString for each element in an array and concats them to one string
	public String getStringForArray(ArrayList<results> passed_arrayList){
		String line = "";
		for(int i = 0; i < passed_arrayList.size(); i++){
			line += passed_arrayList.get(i).toString() + " ";
		}
		
		return line;
	}

}
