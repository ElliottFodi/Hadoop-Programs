import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.ArrayWritable;
import org.apache.hadoop.io.BooleanWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.MapWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;

/*
 * TeamWritable Class
 * Same as the team class except using Hadoop writable primitives 
 * so the class can be serialized and sent from the mapper to the reducer  
 */

class TeamWritable implements Writable {
    ArrayWritable members; // * IntWritable
    Text teamId; // *
    Text matchId; // *
    BooleanWritable winner; // *
    ArrayWritable gold; // * IntWritable
    ArrayWritable avgTeamLevel; // * IntWritable
    ArrayWritable totalTeamXp; // * IntWritable
    ArrayWritable minionsKilled; // * IntWritable
    ArrayWritable jungleMinionsKilled; // * IntWritable
    ArrayWritable championKills; // * IntWritable
    ArrayWritable wardsPlaced; // * IntWritable
    ArrayWritable healthPortionsBought; // * IntWritable
    ArrayWritable manaPortionsBought; // * IntWritable
    ArrayWritable visionWardsBought; // * IntWritable
    ArrayWritable stealthWardsBought; // * IntWritable
    ArrayWritable eliteMonsterKill; // * MapWritable<Text, IntWritable>
    ArrayWritable buildingKill; // * MapWritable<Text, IntWritable>

    //constructor initializing all the arrays 
    public TeamWritable() {
        this.members = new ArrayWritable(IntWritable.class);
        this.teamId = new Text("");
        this.matchId = new Text("");
        this.winner = new BooleanWritable(false);
        this.jungleMinionsKilled = new ArrayWritable(IntWritable.class);
        this.minionsKilled = new ArrayWritable(IntWritable.class);
        gold = new ArrayWritable(IntWritable.class);
        this.championKills = new ArrayWritable(IntWritable.class);
        this.wardsPlaced = new ArrayWritable(IntWritable.class);
        this.avgTeamLevel = new ArrayWritable(IntWritable.class);
        this.totalTeamXp = new ArrayWritable(IntWritable.class);
        this.manaPortionsBought = new ArrayWritable(IntWritable.class);
        this.healthPortionsBought = new ArrayWritable(IntWritable.class);
        this.visionWardsBought = new ArrayWritable(IntWritable.class);
        this.stealthWardsBought = new ArrayWritable(IntWritable.class);
        this.buildingKill = new ArrayWritable(MapWritable.class);
        this.eliteMonsterKill = new ArrayWritable(MapWritable.class);
    }

    //constructor initializing all the arrays to there corresponding passed values
    public TeamWritable(ArrayWritable gold, // * IntWritable
            ArrayWritable avgTeamLevel, // * IntWritable
            ArrayWritable totalTeamXp, // * IntWritable
            ArrayWritable minionsKilled, // * IntWritable
            ArrayWritable jungleMinionsKilled, // * IntWritable
            ArrayWritable championKills, // * IntWritable
            ArrayWritable wardsPlaced, // * IntWritable
            ArrayWritable healthPortionsBought, // * IntWritable
            ArrayWritable manaPortionsBought, // * IntWritable
            ArrayWritable visionWardsBought, // * IntWritable
            ArrayWritable stealthWardsBought, // * IntWritable
            ArrayWritable eliteMonsterKill, // * MapWritable<Text, IntWritable>
            ArrayWritable buildingKill) {
        this.gold = gold;
        this.avgTeamLevel = avgTeamLevel;
        this.totalTeamXp = totalTeamXp;
        this.minionsKilled = minionsKilled;
        this.jungleMinionsKilled = jungleMinionsKilled;
        this.championKills = championKills;
        this.wardsPlaced = wardsPlaced;
        this.healthPortionsBought = healthPortionsBought;
        this.manaPortionsBought = manaPortionsBought;
        this.visionWardsBought = visionWardsBought;
        this.stealthWardsBought = stealthWardsBought;
        this.eliteMonsterKill = eliteMonsterKill;
        this.buildingKill = buildingKill;
    }

    //method to read the serialized writable objects passed into the reducer
    @Override
    public void readFields(DataInput in) throws IOException {
        members.readFields(in); // * IntWritable
        teamId.readFields(in); // *
        matchId.readFields(in); // *
        winner.readFields(in); // *
        gold.readFields(in); // * IntWritable
        avgTeamLevel.readFields(in); // * IntWritable
        totalTeamXp.readFields(in); // * IntWritable
        minionsKilled.readFields(in); // * IntWritable
        jungleMinionsKilled.readFields(in); // * IntWritable
        championKills.readFields(in); // * IntWritable
        wardsPlaced.readFields(in); // * IntWritable
        healthPortionsBought.readFields(in); // * IntWritable
        manaPortionsBought.readFields(in); // * IntWritable
        visionWardsBought.readFields(in); // * IntWritable
        stealthWardsBought.readFields(in); // * IntWritable
        eliteMonsterKill.readFields(in); // * MapWritable<Text, IntWritable>
        buildingKill.readFields(in); // * MapWritable<Text, IntWritable>
    }

    //method to serialize the objects to be passed from the mapper to the reducer
    @Override
    public void write(DataOutput out) throws IOException {
        members.write(out);
        teamId.write(out);
        matchId.write(out);
        winner.write(out);
        gold.write(out);
        avgTeamLevel.write(out);
        totalTeamXp.write(out);
        minionsKilled.write(out);
        jungleMinionsKilled.write(out);
        championKills.write(out);
        wardsPlaced.write(out);
        healthPortionsBought.write(out);
        manaPortionsBought.write(out);
        visionWardsBought.write(out);
        stealthWardsBought.write(out);
        eliteMonsterKill.write(out);
        buildingKill.write(out);
    }

    //custom toString
    public String toString() {
        return new String("gold =" + gold.toString() + "\n" + "avgTeamLevel ="
                + avgTeamLevel.toString() + "\n" + "totalTeamXp ="
                + totalTeamXp.toString() + "\n" + "minionsKilled ="
                + minionsKilled.toString() + "\n" + "jungleMinionsKilled ="
                + jungleMinionsKilled.toString() + "\n" + "championKills ="
                + championKills.toString() + "\n" + "wardsPlaced ="
                + wardsPlaced.toString() + "\n" + "healthPortionsBought ="
                + healthPortionsBought.toString() + "\n"
                + "manaPortionsBought =" + manaPortionsBought.toString() + "\n"
                + "visionWardsBought =" + visionWardsBought.toString() + "\n"
                + "stealthWardsBought =" + stealthWardsBought.toString() + "\n"
                + "eliteMonsterKill =" + eliteMonsterKill.toString() + "\n"
                + "buildingKill =" + buildingKill.toString());
    }
}

