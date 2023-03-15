import java.util.*;
public class Event {
    private HashMap<String, String> elements = new HashMap<String, String>();

    private String[] elementNames= {"Date","Quarter","SecLeft","AwayTeam","HomeTeam","PlayBy"
            ,"Shooter","ShotType","ShotOutcome","Assister","Blocker"
            ,"Rebounder","ReboundType"
            ,"FreeThrowShooter","FreeThrowOutcome"
            ,"Fouler","FoulType"
            ,"TurnoverPlayer","TurnoverType","TurnoverCauser"
            ,"EnterGame","LeaveGame"};

    public Event(String line){
        String []elementValues = line.split(",",elementNames.length);
        for(int i=0;i<elementNames.length;i++){
            elements.put(elementNames[i],elementValues[i]);
        }
    }
    public String get(String name){
        return elements.get(name);
    }
}