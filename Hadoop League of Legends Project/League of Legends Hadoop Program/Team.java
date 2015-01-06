import java.util.ArrayList;
import java.util.HashMap;


/*
 * Team Class
 * class that holds values for team
 * the index of each array corresponds to the interval being recorded 
 * For example an interval of 5, 10, 15, 20, 25 would use indexs 0, 1, 2, 3, 4
 */

public class Team {
    ArrayList<Integer> members;
    String teamId; // *
    String matchId; // *
    boolean winner; // *
    ArrayList<Integer> gold; // *
    ArrayList<Integer> avgTeamLevel; // *
    ArrayList<Integer> totalTeamXp; // *
    ArrayList<Integer> minionsKilled; // *
    ArrayList<Integer> jungleMinionsKilled; // *
    ArrayList<Integer> championKills; // *
    ArrayList<Integer> wardsPlaced; // *
    ArrayList<Integer> healthPortionsBought; // *
    ArrayList<Integer> manaPortionsBought; // *
    ArrayList<Integer> visionWardsBought;
    ArrayList<Integer> stealthWardsBought;
    ArrayList<HashMap<String, Integer>> eliteMonsterKill; //
    ArrayList<HashMap<String, Integer>> buildingKill; //

    // team wise
    // events and participants
    // use array list total gold after every 10 frames
    // minions Killed
    // jungle minions killed
    // avg team level team
    // total xp team
    // events
    // CHAMPION_KILL
    // WARDS_PLACED
    // health portions bought ITEM_PURCHASED 2003
    // mana portions bought ITEM_PURCHASED 2004
    // WARD_PURCHASED ITEM_PURCHASED itemId 2043 vision, 2044 stealth wards
    // ELITE_MONSTER_KILL monsterType
    // BUILDINGS_KILL buildingType

    public Team() {
        this.members = new ArrayList<Integer>();
        this.teamId = "";
        this.matchId = "";
        this.winner = false;
        jungleMinionsKilled = new ArrayList<Integer>();
        minionsKilled = new ArrayList<Integer>();
        gold = new ArrayList<Integer>();
        championKills = new ArrayList<Integer>();
        wardsPlaced = new ArrayList<Integer>();
        avgTeamLevel = new ArrayList<Integer>();
        totalTeamXp = new ArrayList<Integer>();
        manaPortionsBought = new ArrayList<Integer>();
        healthPortionsBought = new ArrayList<Integer>();
        visionWardsBought = new ArrayList<Integer>();
        stealthWardsBought = new ArrayList<Integer>();
        buildingKill = new ArrayList<HashMap<String, Integer>>();
        eliteMonsterKill = new ArrayList<HashMap<String, Integer>>();
    }
}
