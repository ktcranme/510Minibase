package BigT;

import java.io.IOException;

import global.MID;
import heap.InvalidSlotNumberException;

public interface Mapview {
    public abstract Map getMap(MID mid) throws InvalidSlotNumberException, IOException;

    public abstract boolean updateMap(MID mid, Map map) throws IOException, InvalidSlotNumberException;

    public abstract MID firstMap() throws IOException;

    public abstract MID nextMap(MID curMid) throws IOException, InvalidSlotNumberException;
}