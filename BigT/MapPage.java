package BigT;

import java.io.IOException;

import global.MID;
import global.RID;
import heap.HFPage;
import heap.InvalidSlotNumberException;

public class MapPage extends HFPage {
    /**
     * wraps around the function firstRecord()
     * 
     * @return MID of first map on page, null if page contains no maps.
     * @exception IOException I/O errors
     * 
     */
    public MID firstMap() throws IOException {
        MID mid = new MID();
        RID rid = new RID();
        rid = firstRecord();
        if (rid == null)
            return null;
        mid.pageNo = rid.pageNo;
        mid.slotNo = rid.slotNo * Mapfile.VERSIONS;
        return mid;
    }

    /**
     * @return MID of next map on the page, null if no more maps exist on the page
     * @param curMid current map ID
     * @exception IOException I/O errors
     */
    public MID nextMap(MID curMid) throws IOException {
        MID mid = new MID();
        RID paramrid = new RID();

        if (curMid.slotNo % 3 < 2) {
            try {
                PhysicalMap m = returnMap(curMid);
                String val = m.getVersion(curMid.slotNo % Mapfile.VERSIONS + 1);
                if (!val.isEmpty()) {
                    mid.pageNo = curMid.pageNo;
                    mid.slotNo = curMid.slotNo + 1;
                    return mid;
                }
            } catch (InvalidSlotNumberException e) {
                // Go ahead and do next steps
            }
        }

        paramrid.pageNo = curMid.pageNo;
        paramrid.slotNo = curMid.slotNo / Mapfile.VERSIONS;
        RID rid = new RID();
        rid = nextRecord(paramrid);
        if (rid == null)
            return null;
        mid.pageNo = rid.pageNo;
        mid.slotNo = rid.slotNo * Mapfile.VERSIONS;
        return mid;
    }

}