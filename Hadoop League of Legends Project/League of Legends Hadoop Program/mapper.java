
//import java.io.DataInput;
//import java.io.DataOutput;
//import java.io.FileReader;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;
import java.util.TreeSet;


//import org.apache.hadoop.io.ArrayWritable;
//import org.apache.hadoop.io.ArrayWritable;
//import org.apache.hadoop.io.BooleanWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.MapWritable;
import org.apache.hadoop.io.Text;
//import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapreduce.Mapper;
//import org.apache.hadoop.mapreduce.Mapper.Context;
//import org.json.JSONException;
import org.json.simple.JSONArray;
import org.json.simple.JSONObject;
import org.json.simple.parser.JSONParser;
import org.json.simple.parser.ParseException;


public class mapper extends Mapper<LongWritable, Text, Text, TeamWritable> {
    
    public void map(LongWritable key, Text value,  Context context) throws IOException, InterruptedException {
    
        // json parser object to read the json files in java
    	JSONParser jp = new JSONParser();
    	
        // the two team objects that we are filling here
        Team teamA = new Team();
        Team teamB = new Team();

        
        //parse the JSON file and retrieve the elements specified bellow 
        try {
            JSONObject jsonObj = (JSONObject) jp.parse(value.toString());
            JSONArray participants = (JSONArray) jsonObj.get("participants");
            JSONArray teams = (JSONArray) jsonObj.get("teams");
            JSONObject timelineObj = (JSONObject) jp.parse(jsonObj.get(
                    "timeline").toString());
            JSONArray frames = (JSONArray) timelineObj.get("frames");

            // objects to store team members separately
            TreeSet<Integer> aTeamMembers = new TreeSet<Integer>();
            TreeSet<Integer> bTeamMembers = new TreeSet<Integer>();

            for (int i = 0; i < participants.size(); i++) {
                JSONObject participant = (JSONObject) jp.parse(participants.get(i).toString());
                if (participant.get("teamId").toString().equals("100")) {
                    aTeamMembers.add(Integer.parseInt(participant.get("participantId").toString()));
                } else {
                    bTeamMembers.add(Integer.parseInt(participant.get("participantId").toString()));
                }
            }

            // checking for the winning team
            JSONObject teamObj = (JSONObject) jp.parse(teams.get(0).toString());
            if (teamObj.get("teamId").toString().equals("100")) {
                if (teamObj.get("winner").toString().equals("true")) {
                    teamA.winner = true;
                } else {
                    teamB.winner = true;
                }
            } else {
                if (teamObj.get("winner").toString().equals("true")) {
                    teamB.winner = true;
                } else {
                    teamA.winner = true;
                }
            }

            // initializing objects to hold the data to be collected from the match
            // file
            JSONObject frame = null, pframe = null, participant = null, event = null;
            JSONArray events = null;// (JSONArray) timelineObj.get("frames");
            int championKillsA = 0, championKillsB = 0, wardsA = 0, wardsB = 0;
            int healthPortionsA = 0, healthPortionsB = 0, manaPortionsA = 0, manaPortionsB = 0;
            int visionWardA = 0, visionWardB = 0, stealthWardA = 0, stealthWardB = 0;
            HashMap<String, Integer> buildingsA = new HashMap<String, Integer>();
            HashMap<String, Integer> eliteMontersA = new HashMap<String, Integer>();
            HashMap<String, Integer> buildingsB = new HashMap<String, Integer>();
            HashMap<String, Integer> eliteMontersB = new HashMap<String, Integer>();

            // reading each frame for events that may have occurred in that minute
            // to update the variables holding data
            for (int i = 1; i < frames.size(); i++) {
                frame = (JSONObject) jp.parse(frames.get(i).toString());
                pframe = (JSONObject) jp.parse(frame.get("participantFrames").toString());
                events = (JSONArray) frame.get("events");
                int teamLevelA = 0, goldA = 0, xpA = 0, minionsKilledA = 0, jungleMinionsKilledA = 0;
                int teamLevelB = 0, goldB = 0, xpB = 0, minionsKilledB = 0, jungleMinionsKilledB = 0;

                for (int j = 1; j <= aTeamMembers.size() + bTeamMembers.size(); j++) {
                    participant = (JSONObject) jp.parse(pframe.get("" + j).toString());

                    if (aTeamMembers.contains(j)) {
                        goldA += Integer.parseInt(participant.get("totalGold")
                                .toString());
                        teamLevelA += Integer.parseInt(participant.get("level")
                                .toString());
                        minionsKilledA += Integer.parseInt(participant.get(
                                "minionsKilled").toString());
                        jungleMinionsKilledA += Integer.parseInt(participant
                                .get("jungleMinionsKilled").toString());
                        xpA += Integer.parseInt(participant.get("xp")
                                .toString());
                    } else {
                        goldB += Integer.parseInt(participant.get("totalGold")
                                .toString());
                        teamLevelB += Integer.parseInt(participant.get("level")
                                .toString());
                        minionsKilledB += Integer.parseInt(participant.get(
                                "minionsKilled").toString());
                        jungleMinionsKilledB += Integer.parseInt(participant
                                .get("jungleMinionsKilled").toString());
                        xpB += Integer.parseInt(participant.get("xp")
                                .toString());
                    }
                }
                
                // there can be frames in which the events array is missing
                // this means that the minute did not have any events to report
                // hence the events!= null check
                if(events != null)
                for (int j = 0; j < events.size(); j++) {
                    event = (JSONObject) jp.parse(events.get(j).toString());
                    String eventType = event.get("eventType").toString();
                    if (eventType.equals("CHAMPION_KILL")) {
                        if (aTeamMembers.contains(Integer.parseInt(event.get(
                                "killerId").toString()))) {
                            championKillsA++;
                        } else
                            championKillsB++;
                    } else if (eventType.equals("WARD_PLACED")) {
                        if (aTeamMembers.contains(Integer.parseInt(event.get(
                                "creatorId").toString()))) {
                            wardsA++;
                        } else
                            wardsB++;
                    } else if (eventType.equals("ITEM_PURCHASED")) {
                        int itemId = Integer.parseInt(event.get("itemId")
                                .toString());
                        if (itemId == 2003) {
                            if (aTeamMembers.contains(Integer.parseInt(event
                                    .get("participantId").toString()))) {
                                healthPortionsA++;
                            } else
                                healthPortionsB++;
                        } else if (itemId == 2004) {
                            if (aTeamMembers.contains(Integer.parseInt(event
                                    .get("participantId").toString()))) {
                                manaPortionsA++;
                            } else
                                manaPortionsB++;
                        } else if (itemId == 2043) {
                            if (aTeamMembers.contains(Integer.parseInt(event
                                    .get("participantId").toString()))) {
                                visionWardA++;
                            } else
                                visionWardB++;
                        } else if (itemId == 2044) {
                            if (aTeamMembers.contains(Integer.parseInt(event
                                    .get("participantId").toString()))) {
                                stealthWardA++;
                            } else
                                stealthWardB++;
                        }
                    } else if (eventType.equals("BUILDING_KILL")) {
                        String buildingType = event.get("buildingType")
                                .toString();
                        if (buildingType.equals("TOWER_BUILDING")) {
                        	buildingType = event.get("towerType").toString();
                        	}
                        //if (aTeamMembers.contains(Integer.parseInt(event.get(
                        if(Integer.parseInt(event.get("teamId").toString()) == 200){		
                               // "killerId").toString()))) {
                            if (buildingsA.containsKey(buildingType)) {
                                int count = buildingsA.get(buildingType);
                                count++;
                                buildingsA.put(buildingType, count);
                            } else {
                                buildingsA.put(buildingType, 1);
                            }
                        } else {
                            if (buildingsB.containsKey(buildingType)) {
                                int count = buildingsB.get(buildingType);
                                count++;
                                buildingsB.put(buildingType, count);
                            } else {
                                buildingsB.put(buildingType, 1);
                            }
                        }
                    } else if (eventType.equals("ELITE_MONSTER_KILL")) {
                        
                        String monsterType = event.get("monsterType")
                                .toString();
                        if (aTeamMembers.contains(Integer.parseInt(event.get(
                                "killerId").toString()))) {
                            if (eliteMontersA.containsKey(monsterType)) {
                                int count = eliteMontersA.get(monsterType);
                                count++;
                                eliteMontersA.put(monsterType, count);
                            } else {
                                eliteMontersA.put(monsterType, 1);
                            }
                        } else {
                            if (eliteMontersB.containsKey(monsterType)) {
                                int count = eliteMontersB.get(monsterType);
                                count++;
                                eliteMontersB.put(monsterType, count);
                            } else {
                                eliteMontersB.put(monsterType, 1);
                            }
                        }
                    }
                }
                
                // collecting metrics based on the interval of 5
                if ( (i+1) % 5 == 0 || i == frames.size() - 1){
                    teamA.gold.add(goldA);
                    teamA.avgTeamLevel.add(teamLevelA);
                    teamA.minionsKilled.add(minionsKilledA);
                    teamA.jungleMinionsKilled.add(jungleMinionsKilledA);
                    teamA.totalTeamXp.add(xpA);
                    teamA.championKills.add(championKillsA);
                    teamA.wardsPlaced.add(wardsA);
                    teamA.manaPortionsBought.add(manaPortionsA);
                    teamA.healthPortionsBought.add(healthPortionsA);
                    teamA.stealthWardsBought.add(stealthWardA);
                    teamA.visionWardsBought.add(visionWardA);
                    teamA.eliteMonsterKill.add(new HashMap<String, Integer>(
                            eliteMontersA));
                    teamA.buildingKill.add(new HashMap<String, Integer>(
                            buildingsA));

                    teamB.gold.add(goldB);
                    teamB.avgTeamLevel.add(teamLevelB);
                    teamB.minionsKilled.add(minionsKilledB);
                    teamB.jungleMinionsKilled.add(jungleMinionsKilledB);
                    teamB.totalTeamXp.add(xpB);
                    teamB.championKills.add(championKillsB);
                    teamB.wardsPlaced.add(wardsB);
                    teamB.manaPortionsBought.add(manaPortionsB);
                    teamB.healthPortionsBought.add(healthPortionsB);
                    teamB.stealthWardsBought.add(stealthWardB);
                    teamB.visionWardsBought.add(visionWardB);
                    teamB.eliteMonsterKill.add(new HashMap<String, Integer>(
                            eliteMontersB));
                    teamB.buildingKill.add(new HashMap<String, Integer>(
                            buildingsB));
                }
            }
            teamA.members.addAll(aTeamMembers);
            teamB.members.addAll(bTeamMembers);

            // Writable Objects
            TeamWritable teamAWritable = makeWritableTeam(teamA);
            TeamWritable teamBWritable = makeWritableTeam(teamB);
            
            //emit winner or loser 
            if(teamA.winner == true){
            	//emit TeamA winner value
            	context.write(new Text("winner"), teamAWritable);
            	//emit TeamB loser value
            	context.write(new Text("loser"), teamBWritable);
            }else{
            	//emit TeamB winner value
            	context.write(new Text("winner"), teamBWritable);
            	//emit TeamA loser value
            	context.write(new Text("loser"), teamAWritable);
            }
            
        } catch (ParseException e1) {
            e1.printStackTrace();
        }
    } //END MAP FUNCTION

    //convert an arrayList to an arrayWritable
    public static IntWritable[] convert(ArrayList<Integer> items) {
        IntWritable[] intWritableArr = new IntWritable[items.size()];
        int i = 0;
        for (Integer item : items) {
            intWritableArr[i] = new IntWritable(item);
            i++;
        }
        return intWritableArr;
    }
    

    //convert a map to a mapWritable
    public static MapWritable convert(HashMap<String, Integer> items) {
        MapWritable mapWritable = new MapWritable();
        for (Map.Entry<String, Integer> item : items.entrySet()) {
            mapWritable.put(new Text(item.getKey()),
                    new IntWritable(item.getValue()));
        }
        return mapWritable;
    }

    // make TeamWritable Object
    public static TeamWritable makeWritableTeam(Team team) {
        TeamWritable teamWritable = new TeamWritable();

        teamWritable.members.set(convert(team.members));
        teamWritable.teamId.set(team.teamId);
        teamWritable.matchId.set(team.matchId);
        teamWritable.winner.set(team.winner);
        teamWritable.gold.set(convert(team.gold));
        teamWritable.avgTeamLevel.set(convert(team.avgTeamLevel));
        teamWritable.totalTeamXp.set(convert(team.totalTeamXp));
        teamWritable.minionsKilled.set(convert(team.minionsKilled));
        teamWritable.jungleMinionsKilled.set(convert(team.jungleMinionsKilled));
        teamWritable.championKills.set(convert(team.championKills));
        teamWritable.wardsPlaced.set(convert(team.wardsPlaced));
        teamWritable.healthPortionsBought.set(convert(team.healthPortionsBought));
        teamWritable.manaPortionsBought.set(convert(team.manaPortionsBought));
        teamWritable.visionWardsBought.set(convert(team.visionWardsBought));
        teamWritable.stealthWardsBought.set(convert(team.stealthWardsBought));

        MapWritable[] mapWritableArr = new MapWritable[team.eliteMonsterKill.size()];
        int i = 0;
        for (HashMap<String, Integer> map : team.eliteMonsterKill) {
            mapWritableArr[i++] = convert(map);
        }
        teamWritable.eliteMonsterKill.set(mapWritableArr);

        
        mapWritableArr = new MapWritable[team.buildingKill.size()];
        i = 0;
        for (HashMap<String, Integer> map : team.buildingKill) {
            mapWritableArr[i++] = convert(map);
        }
        teamWritable.buildingKill.set(mapWritableArr);
        mapWritableArr = null;

        return teamWritable;
    }
}
