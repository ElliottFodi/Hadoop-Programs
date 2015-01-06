import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;
import java.util.Map.Entry;
import org.apache.hadoop.io.ArrayWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.MapWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapreduce.Reducer;


public class reducer extends Reducer<Text, TeamWritable, String, resultsAggregator> {
	
	
	aggregator aggregate = null;
	resultsAggregator resultAggregate = null;
	
	
	public void reduce(Text key, Iterable<TeamWritable> values, Context context) throws IOException, InterruptedException {

		//number of winners analyzed 
		int numberOfValues = 0;
		
		//number of intervals 
		int intervals = 0;
		
		aggregate = new aggregator();
		
		//see if the reducer is a winner or loser 
		String winLoss = key.toString();
		boolean winner;
		if(winLoss.equals("winner") == true){
			winner = true;
		}else{
			winner = false;
		}
		
		
		for(TeamWritable val : values){
			
			//convert to team
			Team team = makeTeam(val);
			
			//initialize the aggregate arrays == to the intervals being used 
			if(intervals == 0){
				intervals = team.gold.size();
				for(int i = 1; i < intervals; i++){
					aggregate.addHashMapToArray();
				}
			}
			
			//check to make sure intervals is set correctly 
			if(team.gold.size() == intervals){
				
				//add the values from the mapper to there corresponding hash map in the appropriate array 
				//for the interval "i"
				for( int i = 0; i < intervals; i++){
					aggregate.addValue("gold", i, team.gold.get(i));
					aggregate.addValue("avgTeamLevel", i, team.avgTeamLevel.get(i));
					aggregate.addValue("totalTeamXP", i, team.totalTeamXp.get(i));
					aggregate.addValue("minionsKilled", i, team.minionsKilled.get(i));
					aggregate.addValue("jungleMinionsKilled", i, team.jungleMinionsKilled.get(i));
					aggregate.addValue("championsKilled", i, team.championKills.get(i));
					aggregate.addValue("wardsPlaced", i, team.wardsPlaced.get(i));
					aggregate.addValue("healthPotionsBought", i, team.healthPortionsBought.get(i));
					aggregate.addValue("manaPotionsBought", i, team.manaPortionsBought.get(i));
					aggregate.addValue("visionWardsBought", i, team.visionWardsBought.get(i));
					aggregate.addValue("stealthWardsBought", i, team.stealthWardsBought.get(i));
					
				}
				
				// these values are passed as an array of hash maps so they are handled differently
				
				//the values for these hash maps are added to separate hash maps 
				//there is a hash map for each key in the passed hash map... this is possable because the 
				//keys in the hash map are fixed 
				int eliteCount = 0;
				for(HashMap<String, Integer> hm : team.eliteMonsterKill){
					
					for( Entry<String, Integer> monsters : hm.entrySet()){
						aggregate.addValue(monsters.getKey(), eliteCount, monsters.getValue());
					}
					eliteCount++;
				}
				
				int buildingCount = 0;
				for(HashMap<String, Integer> hm : team.buildingKill){
					
					for( Entry<String, Integer> buildings : hm.entrySet()){
						aggregate.addValue(buildings.getKey(), buildingCount, buildings.getValue());
					}
					buildingCount++;
				}
				
				
				//Increment the number of matches processed 
				numberOfValues++;
			}
		}
		
		//initialize the class to hold all the results from the following functions
		resultAggregate = new resultsAggregator(intervals);

		//get the mean, median, mode, and standard deviation for all the hash maps
		setResults(aggregate, "gold", intervals, numberOfValues, resultAggregate);
		setResults(aggregate, "avgTeamLevel", intervals, numberOfValues, resultAggregate);
		setResults(aggregate, "totalTeamXP", intervals, numberOfValues, resultAggregate);
		setResults(aggregate, "minionsKilled", intervals, numberOfValues, resultAggregate);
		setResults(aggregate, "jungleMinionsKilled", intervals, numberOfValues, resultAggregate);
		setResults(aggregate, "championsKilled", intervals, numberOfValues, resultAggregate);
		setResults(aggregate, "wardsPlaced", intervals, numberOfValues, resultAggregate);
		setResults(aggregate, "healthPotionsBought", intervals, numberOfValues, resultAggregate);
		setResults(aggregate, "manaPotionsBought", intervals, numberOfValues, resultAggregate);
		setResults(aggregate, "visionWardsBought", intervals, numberOfValues, resultAggregate);
		setResults(aggregate, "stealthWardsBought", intervals, numberOfValues, resultAggregate);
		
		setResults(aggregate, "RED_LIZARD", intervals, numberOfValues, resultAggregate);
		setResults(aggregate, "BLUE_GOLEM", intervals, numberOfValues, resultAggregate);
		setResults(aggregate, "DRAGON", intervals, numberOfValues, resultAggregate);
		setResults(aggregate, "BARON_NASHOR", intervals, numberOfValues, resultAggregate);
		
		setResults(aggregate, "INHIBITOR", intervals, numberOfValues, resultAggregate);
		setResults(aggregate, "TOWER", intervals, numberOfValues, resultAggregate);
		
		
		setResults(aggregate, "BASE_TURRET", intervals, numberOfValues, resultAggregate);
		setResults(aggregate, "FOUNTAIN_TURRET", intervals, numberOfValues, resultAggregate);
		setResults(aggregate, "INNER_TURRET", intervals, numberOfValues, resultAggregate);
		setResults(aggregate, "NEXUS_TURRET", intervals, numberOfValues, resultAggregate);
		setResults(aggregate, "OUTER_TURRET", intervals, numberOfValues, resultAggregate);
		setResults(aggregate, "UNDEFINED_TURRET", intervals, numberOfValues, resultAggregate);
		
		//output the final results from the reducer
		if(winner == true){
			context.write("winner", resultAggregate);
		}else{
			context.write("loser", resultAggregate);
		}
		
		
	}
	
	//convert a writableArray to an ArrayList
    private static ArrayList<Integer> convert(ArrayWritable items) {
    	IntWritable[] intarray = (IntWritable[]) items.toArray();
    	//IntWritable[] intarray = (IntWritable[]) items.get();
    	ArrayList<Integer> arraylist = new ArrayList<Integer>();
    	for(int i = 0; i < intarray.length; i++){
    		arraylist.add(intarray[i].get());
    	}

        return arraylist;
    }

    //convert a writableHashMap to a Hash Map
    private static HashMap<String, Integer> convert(MapWritable items) {
    	
    	HashMap<String, Integer> hashmap = new HashMap<String, Integer>();
    	
    	if(items.isEmpty() == false){	
	    	for(Map.Entry<Writable, Writable> item : items.entrySet()){
	    		hashmap.put(item.getKey().toString(), Integer.parseInt(item.getValue().toString()));
	    	}
    	}
    	
        return hashmap;
    }
	
    //create a new team object to place the converted values into
    //and add the values to the team object 
    private static Team makeTeam(TeamWritable teamWritable) {
        Team team = new Team();
        
        team.members = convert(teamWritable.members);
        team.teamId = teamWritable.teamId.toString();
        team.matchId = teamWritable.matchId.toString();
        team.winner = Boolean.parseBoolean(teamWritable.matchId.toString());
        team.gold = convert(teamWritable.gold);
        team.avgTeamLevel = convert(teamWritable.avgTeamLevel);
        team.totalTeamXp = convert(teamWritable.totalTeamXp);
        team.minionsKilled = convert(teamWritable.minionsKilled);
        team.jungleMinionsKilled = convert(teamWritable.jungleMinionsKilled);
        team.championKills = convert(teamWritable.championKills);
        team.wardsPlaced = convert(teamWritable.wardsPlaced);
        team.healthPortionsBought = convert(teamWritable.healthPortionsBought);
        team.manaPortionsBought = convert(teamWritable.manaPortionsBought);
        team.visionWardsBought = convert(teamWritable.visionWardsBought);
        team.stealthWardsBought = convert(teamWritable.stealthWardsBought);
        

        ArrayWritable eliteMonsterArray = teamWritable.eliteMonsterKill;
        MapWritable[] eliteMonsterMaps = (MapWritable[]) eliteMonsterArray.toArray();
        
        for(int i = 0; i < eliteMonsterMaps.length; i++){
        	team.eliteMonsterKill.add(convert(eliteMonsterMaps[i]));
        }
        
        ArrayWritable buildingArray = teamWritable.buildingKill;
        MapWritable[] building = (MapWritable[]) buildingArray.toArray();
        
        for(int i = 0; i < building.length; i++){
        	team.buildingKill.add(convert(building[i]));
        }
        

        return team;
    }
    
    //gets the mean, median, mode, and standard deviation for a hash map 
    public static void setResults(aggregator passed_aggregate, String passed_name, 
    		int passed_interval, int passed_numberOfValues, resultsAggregator passed_resultsAggregator){
		for(int i = 0; i < passed_interval; i++){			
			long mean = passed_aggregate.mean(passed_name, i, passed_numberOfValues);
			int median = passed_aggregate.median(passed_name, i);
			
			int mode = passed_aggregate.mode(passed_name, i);
			
			long sd = passed_aggregate.standardDeviation(passed_name, i, passed_numberOfValues);
			passed_resultsAggregator.setResult(passed_name, i, mean, median, mode, sd);
		
		}
    	
    }

}
