package BigT;

import java.io.IOException;

import global.MID;
import heap.InvalidSlotNumberException;
import heap.InvalidUpdateException;

public interface Mapview {
    public abstract Map getMap(MID mid) throws InvalidSlotNumberException, IOException;

    public abstract MID updateMap(MID mid, Map map) throws IOException, InvalidSlotNumberException, InvalidUpdateException;

    public abstract MID firstMap() throws IOException;

    public abstract MID nextMap(MID curMid) throws IOException, InvalidSlotNumberException;
}