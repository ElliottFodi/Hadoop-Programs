import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map.Entry;

public class aggregator {
	
	//the intervals gathered by the mapper, for example 5, 10, 15, 20, 25 would be 5 intervals
	private int intervals;

	//Arrays of Hash Maps, where the array index corrisponds to the interval
	//the Hash map key == the value passed by the mapper, value == how many times that value occurred 
	//across the games sampled
	
	//general stats
	ArrayList<HashMap<Integer, Integer>> gold = new ArrayList<HashMap<Integer, Integer>>();
	ArrayList<HashMap<Integer, Integer>> avgTeamLevel = new ArrayList<HashMap<Integer, Integer>>();
	ArrayList<HashMap<Integer, Integer>> totalTeamXP = new ArrayList<HashMap<Integer, Integer>>();
	ArrayList<HashMap<Integer, Integer>> minionsKilled = new ArrayList<HashMap<Integer, Integer>>();
	ArrayList<HashMap<Integer, Integer>> jungleMinionsKilled = new ArrayList<HashMap<Integer, Integer>>();
	ArrayList<HashMap<Integer, Integer>> championsKilled = new ArrayList<HashMap<Integer, Integer>>();
	ArrayList<HashMap<Integer, Integer>> wardsPlaced = new ArrayList<HashMap<Integer, Integer>>();
	ArrayList<HashMap<Integer, Integer>> healthPotionsBought = new ArrayList<HashMap<Integer, Integer>>();
	ArrayList<HashMap<Integer, Integer>> manaPotionsBought = new ArrayList<HashMap<Integer, Integer>>();
	ArrayList<HashMap<Integer, Integer>> visionWardsBought = new ArrayList<HashMap<Integer, Integer>>();
	ArrayList<HashMap<Integer, Integer>> stealthWardsBought = new ArrayList<HashMap<Integer, Integer>>();
	
	// elite monsters
	ArrayList<HashMap<Integer, Integer>> RED_LIZARD = new ArrayList<HashMap<Integer, Integer>>();
	ArrayList<HashMap<Integer, Integer>> BLUE_GOLEM = new ArrayList<HashMap<Integer, Integer>>();
	ArrayList<HashMap<Integer, Integer>> DRAGON = new ArrayList<HashMap<Integer, Integer>>();
	ArrayList<HashMap<Integer, Integer>> BARON_NASHOR = new ArrayList<HashMap<Integer, Integer>>();
	
	// buildings
	ArrayList<HashMap<Integer, Integer>> inhibitor = new ArrayList<HashMap<Integer, Integer>>();
	ArrayList<HashMap<Integer, Integer>> tower = new ArrayList<HashMap<Integer, Integer>>();

	// should have buildings
	ArrayList<HashMap<Integer, Integer>> base_turret = new ArrayList<HashMap<Integer, Integer>>();
	ArrayList<HashMap<Integer, Integer>> fountain_turret = new ArrayList<HashMap<Integer, Integer>>();
	ArrayList<HashMap<Integer, Integer>> inner_turret = new ArrayList<HashMap<Integer, Integer>>();
	ArrayList<HashMap<Integer, Integer>> nexus_turret = new ArrayList<HashMap<Integer, Integer>>();
	ArrayList<HashMap<Integer, Integer>> outer_turret = new ArrayList<HashMap<Integer, Integer>>();
	ArrayList<HashMap<Integer, Integer>> undefined_turret = new ArrayList<HashMap<Integer, Integer>>();

	//Constructor initializing the arrays
	public aggregator() {
		
		//general stats 
		gold.add(new HashMap<Integer, Integer>());
		avgTeamLevel.add(new HashMap<Integer, Integer>());
		totalTeamXP.add(new HashMap<Integer, Integer>());
		minionsKilled.add(new HashMap<Integer, Integer>());
		jungleMinionsKilled.add(new HashMap<Integer, Integer>());
		championsKilled.add(new HashMap<Integer, Integer>());
		wardsPlaced.add(new HashMap<Integer, Integer>());
		healthPotionsBought.add(new HashMap<Integer, Integer>());
		manaPotionsBought.add(new HashMap<Integer, Integer>());
		visionWardsBought.add(new HashMap<Integer, Integer>());
		stealthWardsBought.add(new HashMap<Integer, Integer>());

		// elite monsters
		RED_LIZARD.add(new HashMap<Integer, Integer>());
		BLUE_GOLEM.add(new HashMap<Integer, Integer>());
		DRAGON.add(new HashMap<Integer, Integer>());
		BARON_NASHOR.add(new HashMap<Integer, Integer>());

		// building types
		inhibitor.add(new HashMap<Integer, Integer>());
		tower.add(new HashMap<Integer, Integer>());

		// tower types
		base_turret.add(new HashMap<Integer, Integer>());
		fountain_turret.add(new HashMap<Integer, Integer>());
		inner_turret.add(new HashMap<Integer, Integer>());
		nexus_turret.add(new HashMap<Integer, Integer>());
		outer_turret.add(new HashMap<Integer, Integer>());
		undefined_turret.add(new HashMap<Integer, Integer>());
		
	}
	
	//Constructor initializing each array with the the value of passed interval == to the ampount of
	//hash maps in the array
	public aggregator(int passed_interval) {
		intervals = passed_interval;

		// add hash maps to each array based on the interval being sampled
		for (int i = 0; i < passed_interval; i++) {
			gold.add(new HashMap<Integer, Integer>());
			avgTeamLevel.add(new HashMap<Integer, Integer>());
			totalTeamXP.add(new HashMap<Integer, Integer>());
			minionsKilled.add(new HashMap<Integer, Integer>());
			jungleMinionsKilled.add(new HashMap<Integer, Integer>());
			championsKilled.add(new HashMap<Integer, Integer>());
			wardsPlaced.add(new HashMap<Integer, Integer>());
			healthPotionsBought.add(new HashMap<Integer, Integer>());
			manaPotionsBought.add(new HashMap<Integer, Integer>());
			visionWardsBought.add(new HashMap<Integer, Integer>());
			stealthWardsBought.add(new HashMap<Integer, Integer>());

			// elite monsters
			RED_LIZARD.add(new HashMap<Integer, Integer>());
			BLUE_GOLEM.add(new HashMap<Integer, Integer>());
			DRAGON.add(new HashMap<Integer, Integer>());
			BARON_NASHOR.add(new HashMap<Integer, Integer>());

			// building types
			inhibitor.add(new HashMap<Integer, Integer>());
			tower.add(new HashMap<Integer, Integer>());

			// tower types
			base_turret.add(new HashMap<Integer, Integer>());
			fountain_turret.add(new HashMap<Integer, Integer>());
			inner_turret.add(new HashMap<Integer, Integer>());
			nexus_turret.add(new HashMap<Integer, Integer>());
			outer_turret.add(new HashMap<Integer, Integer>());
			undefined_turret.add(new HashMap<Integer, Integer>());
		}
	}
	
	//Adds a hash map to every array
	public void addHashMapToArray(){
		gold.add(new HashMap<Integer, Integer>());
		avgTeamLevel.add(new HashMap<Integer, Integer>());
		totalTeamXP.add(new HashMap<Integer, Integer>());
		minionsKilled.add(new HashMap<Integer, Integer>());
		jungleMinionsKilled.add(new HashMap<Integer, Integer>());
		championsKilled.add(new HashMap<Integer, Integer>());
		wardsPlaced.add(new HashMap<Integer, Integer>());
		healthPotionsBought.add(new HashMap<Integer, Integer>());
		manaPotionsBought.add(new HashMap<Integer, Integer>());
		visionWardsBought.add(new HashMap<Integer, Integer>());
		stealthWardsBought.add(new HashMap<Integer, Integer>());

		// elite monsters
		RED_LIZARD.add(new HashMap<Integer, Integer>());
		BLUE_GOLEM.add(new HashMap<Integer, Integer>());
		DRAGON.add(new HashMap<Integer, Integer>());
		BARON_NASHOR.add(new HashMap<Integer, Integer>());

		// building types
		inhibitor.add(new HashMap<Integer, Integer>());
		tower.add(new HashMap<Integer, Integer>());

		// tower types
		base_turret.add(new HashMap<Integer, Integer>());
		fountain_turret.add(new HashMap<Integer, Integer>());
		inner_turret.add(new HashMap<Integer, Integer>());
		nexus_turret.add(new HashMap<Integer, Integer>());
		outer_turret.add(new HashMap<Integer, Integer>());
		undefined_turret.add(new HashMap<Integer, Integer>());
	}

	//adds a value to an array, based on the name of the array passed
	public void addValue(String passed_arrayName, int passed_interval,
			int passed_value) {

		switch (passed_arrayName) {

		case "gold":
			addToHashMap(gold, passed_interval, passed_value);
			// if(gold.get(passed_interval).containsKey(passed_value) == true){
			// int temp = 0;
			// temp = gold.get(passed_interval).get(passed_value);
			// temp++;
			// gold.get(passed_interval).put(passed_value, temp);
			// }else{
			// gold.get(passed_interval).put(passed_value, 1);
			// }
			break;

		case "avgTeamLevel":
			addToHashMap(avgTeamLevel, passed_interval, passed_value);
			break;

		case "totalTeamXP":
			addToHashMap(totalTeamXP, passed_interval, passed_value);
			break;

		case "minionsKilled":
			addToHashMap(minionsKilled, passed_interval, passed_value);
			break;

		case "jungleMinionsKilled":
			addToHashMap(jungleMinionsKilled, passed_interval, passed_value);
			break;

		case "championsKilled":
			addToHashMap(championsKilled, passed_interval, passed_value);
			break;

		case "wardsPlaced":
			addToHashMap(wardsPlaced, passed_interval, passed_value);
			break;

		case "healthPotionsBought":
			addToHashMap(healthPotionsBought, passed_interval, passed_value);
			break;

		case "manaPotionsBought":
			addToHashMap(manaPotionsBought, passed_interval, passed_value);
			break;

		case "visionWardsBought":
			addToHashMap(visionWardsBought, passed_interval, passed_value);
			break;

		case "stealthWardsBought":
			addToHashMap(stealthWardsBought, passed_interval, passed_value);
			break;

		case "RED_LIZARD":
			addToHashMap(RED_LIZARD, passed_interval, passed_value);
			break;

		case "BLUE_GOLEM":
			addToHashMap(BLUE_GOLEM, passed_interval, passed_value);
			break;

		case "DRAGON":
			addToHashMap(DRAGON, passed_interval, passed_value);
			break;

		case "BARON_NASHOR":
			addToHashMap(BARON_NASHOR, passed_interval, passed_value);
			break;

		case "INHIBITOR_BUILDING":
			addToHashMap(inhibitor, passed_interval, passed_value);
			break;

		case "TOWER":
			addToHashMap(tower, passed_interval, passed_value);
			break;

		case "BASE_TURRET":
			addToHashMap(base_turret, passed_interval, passed_value);
			break;

		case "FOUNTAIN_TURRET":
			addToHashMap(fountain_turret, passed_interval, passed_value);
			break;

		case "INNER_TURRET":
			addToHashMap(inner_turret, passed_interval, passed_value);
			break;

		case "NEXUS_TURRET":
			addToHashMap(nexus_turret, passed_interval, passed_value);
			break;

		case "OUTER_TURRET":
			addToHashMap(outer_turret, passed_interval, passed_value);
			break;

		case "UNDEFINED_TURRET":
			addToHashMap(undefined_turret, passed_interval, passed_value);
			break;

		default:
			// do nothing invalid input
			break;

		}

	}

	//calculates the mean a hash map, based on the arra name and interval passed
	public long mean(String passed_arrayName, int passed_interval,
			int passed_dataSize) {
		long mean = 0;

		switch (passed_arrayName) {

		case "gold":
			mean = getMean(gold, passed_interval, passed_dataSize);
			// for(Entry<Integer, Integer> val :
			// gold.get(passed_interval).entrySet()){
			// total += val.getKey() * val.getValue();
			// }
			break;

		case "avgTeamLevel":
			mean = getMean(avgTeamLevel, passed_interval, passed_dataSize);
			break;

		case "totalTeamXP":
			mean = getMean(totalTeamXP, passed_interval, passed_dataSize);
			break;

		case "minionsKilled":
			mean = getMean(minionsKilled, passed_interval, passed_dataSize);
			break;

		case "jungleMinionsKilled":
			mean = getMean(jungleMinionsKilled, passed_interval,
					passed_dataSize);
			break;

		case "championsKilled":
			mean = getMean(championsKilled, passed_interval, passed_dataSize);
			break;

		case "wardsPlaced":
			mean = getMean(wardsPlaced, passed_interval, passed_dataSize);
			break;

		case "healthPotionsBought":
			mean = getMean(healthPotionsBought, passed_interval,
					passed_dataSize);
			break;

		case "manaPotionsBought":
			mean = getMean(manaPotionsBought, passed_interval, passed_dataSize);
			break;

		case "visionWardsBought":
			mean = getMean(visionWardsBought, passed_interval, passed_dataSize);
			break;

		case "stealthWardsBought":
			mean = getMean(stealthWardsBought, passed_interval, passed_dataSize);
			break;

		case "RED_LIZARD":
			mean = getMean(RED_LIZARD, passed_interval, passed_dataSize);
			break;

		case "BLUE_GOLEM":
			mean = getMean(BLUE_GOLEM, passed_interval, passed_dataSize);
			break;

		case "DRAGON":
			mean = getMean(DRAGON, passed_interval, passed_dataSize);
			break;

		case "BARON_NASHOR":
			mean = getMean(BARON_NASHOR, passed_interval, passed_dataSize);
			break;

		case "INHIBITOR_BUILDING":
			mean = getMean(inhibitor, passed_interval, passed_dataSize);
			break;

		case "TOWER":
			mean = getMean(tower, passed_interval, passed_dataSize);
			break;

		case "BASE_TURRET":
			mean = getMean(base_turret, passed_interval, passed_dataSize);
			break;

		case "FOUNTAIN_TURRET":
			mean = getMean(fountain_turret, passed_interval, passed_dataSize);
			break;

		case "INNER_TURRET":
			mean = getMean(inner_turret, passed_interval, passed_dataSize);
			break;

		case "NEXUS_TURRET":
			mean = getMean(nexus_turret, passed_interval, passed_dataSize);
			break;

		case "OUTER_TURRET":
			mean = getMean(outer_turret, passed_interval, passed_dataSize);
			break;

		case "UNDEFINED_TURRET":
			mean = getMean(undefined_turret, passed_interval, passed_dataSize);
			break;

		default:
			// do nothing invalid input
			break;
		}

		return mean;
	}

	//calculates the median for a hash map based on the array name and interval
	public int median(String passed_arrayName, int passed_interval) {
		int median = 0;
		switch (passed_arrayName) {

		case "gold":
			median = getMedian(gold, passed_interval);
			// List<Integer> keys = new
			// ArrayList<Integer>(gold.get(passed_interval).keySet());
			// java.util.Collections.sort(keys);
			//
			// List<Integer> values = new
			// ArrayList<Integer>(gold.get(passed_interval).values());
			// int sumOfValues = 0;
			// for(int i = 0; i < values.size(); i++){
			// sumOfValues += values.get(i);
			// }
			//
			// int middle = sumOfValues/2;
			// int counter = 0;
			//
			// for(int i = 0; i < middle; i = counter){
			// counter = gold.get(passed_interval).get(keys.get(i));
			// median = keys.get(i);
			// }
			break;

		case "avgTeamLevel":
			median = getMedian(avgTeamLevel, passed_interval);
			break;

		case "totalTeamXP":
			median = getMedian(totalTeamXP, passed_interval);
			break;

		case "minionsKilled":
			median = getMedian(minionsKilled, passed_interval);
			break;

		case "jungleMinionsKilled":
			median = getMedian(jungleMinionsKilled, passed_interval);
			break;

		case "championsKilled":
			median = getMedian(championsKilled, passed_interval);
			break;

		case "wardsPlaced":
			median = getMedian(wardsPlaced, passed_interval);
			break;

		case "healthPotionsBought":
			median = getMedian(wardsPlaced, passed_interval);
			break;

		case "manaPotionsBought":
			median = getMedian(manaPotionsBought, passed_interval);
			break;

		case "visionWardsBought":
			median = getMedian(visionWardsBought, passed_interval);
			break;

		case "stealthWardsBought":
			median = getMedian(stealthWardsBought, passed_interval);
			break;

		case "RED_LIZARD":
			median = getMedian(RED_LIZARD, passed_interval);
			break;

		case "BLUE_GOLEM":
			median = getMedian(BLUE_GOLEM, passed_interval);
			break;

		case "DRAGON":
			median = getMedian(DRAGON, passed_interval);
			break;

		case "BARON_NASHOR":
			median = getMedian(BARON_NASHOR, passed_interval);
			break;

		case "INHIBITOR_BUILDING":
			median = getMedian(inhibitor, passed_interval);
			break;

		case "TOWER":
			median = getMedian(tower, passed_interval);
			break;

		case "BASE_TURRET":
			median = getMedian(base_turret, passed_interval);
			break;

		case "FOUNTAIN_TURRET":
			median = getMedian(fountain_turret, passed_interval);
			break;

		case "INNER_TURRET":
			median = getMedian(inner_turret, passed_interval);
			break;

		case "NEXUS_TURRET":
			median = getMedian(nexus_turret, passed_interval);
			break;

		case "OUTER_TURRET":
			median = getMedian(outer_turret, passed_interval);
			break;

		case "UNDEFINED_TURRET":
			median = getMedian(undefined_turret, passed_interval);
			break;

		default:
			// do nothing invalid input
			break;

		}
		return median;
	}

	//calculates the mode for a hash map based on the array name and interval 
	public int mode(String passed_arrayName, int passed_interval) {

		int mode = 0;

		switch (passed_arrayName) {

		case "gold":
			mode = getMode(gold, passed_interval);
			// for( Entry<Integer, Integer> val :
			// gold.get(passed_interval).entrySet() ){
			// if(mostOccured < val.getValue()){
			// mode = val.getKey();
			// mostOccured = val.getValue();
			// }
			// }
			break;

		case "avgTeamLevel":
			mode = getMode(avgTeamLevel, passed_interval);
			break;

		case "totalTeamXP":
			mode = getMode(totalTeamXP, passed_interval);
			break;

		case "minionsKilled":
			mode = getMode(minionsKilled, passed_interval);
			break;

		case "jungleMinionsKilled":
			mode = getMode(jungleMinionsKilled, passed_interval);
			break;

		case "championsKilled":
			mode = getMode(championsKilled, passed_interval);
			break;

		case "wardsPlaced":
			mode = getMode(wardsPlaced, passed_interval);
			break;

		case "healthPotionsBought":
			mode = getMode(healthPotionsBought, passed_interval);
			break;

		case "manaPotionsBought":
			mode = getMode(manaPotionsBought, passed_interval);
			break;

		case "visionWardsBought":
			mode = getMode(visionWardsBought, passed_interval);
			break;

		case "stealthWardsBought":
			mode = getMode(stealthWardsBought, passed_interval);
			break;

		case "RED_LIZARD":
			mode = getMode(RED_LIZARD, passed_interval);
			break;

		case "BLUE_GOLEM":
			mode = getMode(BLUE_GOLEM, passed_interval);
			break;

		case "DRAGON":
			mode = getMode(DRAGON, passed_interval);
			break;

		case "BARON_NASHOR":
			mode = getMode(BARON_NASHOR, passed_interval);
			break;

		case "INHIBITOR_BUILDING":
			mode = getMode(inhibitor, passed_interval);
			break;

		case "TOWER":
			mode = getMode(tower, passed_interval);
			break;

		case "BASE_TURRET":
			mode = getMode(base_turret, passed_interval);
			break;

		case "FOUNTAIN_TURRET":
			mode = getMode(fountain_turret, passed_interval);
			break;

		case "INNER_TURRET":
			mode = getMode(inner_turret, passed_interval);
			break;

		case "NEXUS_TURRET":
			mode = getMode(nexus_turret, passed_interval);
			break;

		case "OUTER_TURRET":
			mode = getMode(outer_turret, passed_interval);
			break;

		case "UNDEFINED_TURRET":
			mode = getMode(undefined_turret, passed_interval);
			break;

		default:
			// do nothing invalid input
			break;
		}
		return mode;
	}

	//calculates the standard deviation for a hash map based on the array name and interval
	public long standardDeviation(String passed_arrayName, int passed_interval,
			int passed_dataSize) {

		long SD = 0;
		switch (passed_arrayName) {

		case "gold":
			SD = getStandardDeviation(gold, passed_arrayName, passed_interval,
					passed_dataSize);
			// average = mean(passed_arrayName, passed_interval,
			// passed_dataSize);
			// for(Entry<Integer, Integer> val :
			// gold.get(passed_interval).entrySet()){
			// total += Math.pow((val.getKey() - average), 2) * val.getValue();
			// }
			// SD = Math.sqrt(total/gold.get(passed_interval).size());
			break;

		case "avgTeamLevel":
			SD = getStandardDeviation(avgTeamLevel, passed_arrayName,
					passed_interval, passed_dataSize);
			break;

		case "totalTeamXP":
			SD = getStandardDeviation(totalTeamXP, passed_arrayName,
					passed_interval, passed_dataSize);
			break;

		case "minionsKilled":
			SD = getStandardDeviation(minionsKilled, passed_arrayName,
					passed_interval, passed_dataSize);
			break;

		case "jungleMinionsKilled":
			SD = getStandardDeviation(jungleMinionsKilled, passed_arrayName,
					passed_interval, passed_dataSize);
			break;

		case "championsKilled":
			SD = getStandardDeviation(championsKilled, passed_arrayName,
					passed_interval, passed_dataSize);
			break;

		case "wardsPlaced":
			SD = getStandardDeviation(wardsPlaced, passed_arrayName,
					passed_interval, passed_dataSize);
			break;

		case "healthPotionsBought":
			SD = getStandardDeviation(healthPotionsBought, passed_arrayName,
					passed_interval, passed_dataSize);
			break;

		case "manaPotionsBought":
			SD = getStandardDeviation(manaPotionsBought, passed_arrayName,
					passed_interval, passed_dataSize);
			break;

		case "visionWardsBought":
			SD = getStandardDeviation(visionWardsBought, passed_arrayName,
					passed_interval, passed_dataSize);
			break;

		case "stealthWardsBought":
			SD = getStandardDeviation(stealthWardsBought, passed_arrayName,
					passed_interval, passed_dataSize);
			break;

		case "RED_LIZARD":
			SD = getStandardDeviation(RED_LIZARD, passed_arrayName,
					passed_interval, passed_dataSize);
			break;

		case "BLUE_GOLEM":
			SD = getStandardDeviation(BLUE_GOLEM, passed_arrayName,
					passed_interval, passed_dataSize);
			break;

		case "DRAGON":
			SD = getStandardDeviation(DRAGON, passed_arrayName,
					passed_interval, passed_dataSize);
			break;

		case "BARON_NASHOR":
			SD = getStandardDeviation(BARON_NASHOR, passed_arrayName,
					passed_interval, passed_dataSize);
			break;

		case "INHIBITOR_BUILDING":
			SD = getStandardDeviation(inhibitor, passed_arrayName,
					passed_interval, passed_dataSize);
			break;

		case "TOWER":
			SD = getStandardDeviation(tower, passed_arrayName, passed_interval,
					passed_dataSize);
			break;

		case "BASE_TURRET":
			SD = getStandardDeviation(base_turret, passed_arrayName,
					passed_interval, passed_dataSize);
			break;

		case "FOUNTAIN_TURRET":
			SD = getStandardDeviation(fountain_turret, passed_arrayName,
					passed_interval, passed_dataSize);
			break;

		case "INNER_TURRET":
			SD = getStandardDeviation(inner_turret, passed_arrayName,
					passed_interval, passed_dataSize);
			break;

		case "NEXUS_TURRET":
			SD = getStandardDeviation(nexus_turret, passed_arrayName,
					passed_interval, passed_dataSize);
			break;

		case "OUTER_TURRET":
			SD = getStandardDeviation(outer_turret, passed_arrayName,
					passed_interval, passed_dataSize);
			break;

		case "UNDEFINED_TURRET":
			SD = getStandardDeviation(undefined_turret, passed_arrayName,
					passed_interval, passed_dataSize);
			break;

		default:
			// do nothing invalid input
			break;

		}

		return SD;
	}

	//Generic function to add a value to a hash map at a passed interval
	private void addToHashMap(
			ArrayList<HashMap<Integer, Integer>> passed_array,
			int passed_interval, int passed_value) {
		if (passed_array.get(passed_interval).containsKey(passed_value) == true) {
			int temp = 0;
			temp = passed_array.get(passed_interval).get(passed_value);
			temp++;
			passed_array.get(passed_interval).put(passed_value, temp);
		} else {
			passed_array.get(passed_interval).put(passed_value, 1);
		}
	}

	//Generic function to calculate the mean for a hash map 
	private long getMean(ArrayList<HashMap<Integer, Integer>> passed_array, int passed_interval, int passed_dataSize) {
		long total = 0;
		for (Entry<Integer, Integer> val : passed_array.get(passed_interval).entrySet()) {
			total += val.getKey() * val.getValue();
		}
		return (total / passed_dataSize);
	}

	//generic function to calculate the median for a hash map 
	private int getMedian(ArrayList<HashMap<Integer, Integer>> passed_array, int passed_interval) {
		List<Integer> keys = new ArrayList<Integer>(passed_array.get(passed_interval).keySet());
		java.util.Collections.sort(keys);

		List<Integer> values = new ArrayList<Integer>(passed_array.get(passed_interval).values());
		float sumOfValues = 0;
		for (int i = 0; i < values.size(); i++) {
			sumOfValues += values.get(i);
		}

		int middle = Math.round(sumOfValues / 2);
		
		
		//integer division rounds .5 down to zero
		if(middle == 0 ){
			middle = 1;
		}
		
		int counter = 0;
		int median = 0;
		int i = 0;
		for (i = 0; i < keys.size() && middle > 0; middle = middle - counter, i++) {
			counter = passed_array.get(passed_interval).get(keys.get(i));
			median = keys.get(i);
		}

		return median;
	}

	// generic function to calculate the mode for a hash map
	private int getMode(ArrayList<HashMap<Integer, Integer>> passed_array,
			int passed_interval) {
		int mode = 0;
		int mostOccured = 0;
		for (Entry<Integer, Integer> val : passed_array.get(passed_interval)
				.entrySet()) {
			if (mostOccured < val.getValue()) {
				mode = val.getKey();
				mostOccured = val.getValue();
			}
		}
		return mode;
	}

	// generic function to calculate the standard deviation for a hash map
	private long getStandardDeviation(
			ArrayList<HashMap<Integer, Integer>> passed_array,
			String passed_arrayName, int passed_interval, int passed_dataSize) {
		long average = 0;
		long total = 0;
		double SD = 0;
		average = mean(passed_arrayName, passed_interval, passed_dataSize);
		for (Entry<Integer, Integer> val : passed_array.get(passed_interval)
				.entrySet()) {
			total += Math.pow((val.getKey() - average), 2) * val.getValue();
		}
		if(total == 0){
			SD = 0;
		}else{			
			SD = Math.sqrt(total / passed_array.get(passed_interval).size());
		}
		return (long) SD;
	}
	
	/**
	 * @return the intervals
	 */
	public int getIntervals() {
		return intervals;
	}

}
