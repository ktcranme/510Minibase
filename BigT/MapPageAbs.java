package BigT;

import java.io.IOException;

import global.MID;
import heap.HFPage;
import heap.InvalidSlotNumberException;

public abstract class MapPageAbs extends HFPage {
    public abstract MID firstMap() throws IOException;

    public abstract MID nextMap(MID curMid) throws IOException;

    public abstract Map getMap(MID mid) throws InvalidSlotNumberException, IOException;

    public abstract int deleteMap(MID rid) throws IOException, InvalidSlotNumberException;
}