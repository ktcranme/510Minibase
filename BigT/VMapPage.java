/* File HFPage.java */

package BigT;

import java.io.*;

import global.*;
import heap.HFPage;
import heap.InvalidSlotNumberException;
import diskmgr.*;

public class VMapPage extends HFPage {
    public VMapPage(Page page) {
        super(page);
    }

    public VMapPage() {
        super();
    }

    public Map getMap(MID mid) throws InvalidSlotNumberException, IOException {
        byte[] rec = getRecord(new RID(mid.pageNo, mid.slotNo));
        return new Map(rec, 0);
    }

    public boolean updateMap(MID mid, Map map) throws IOException, InvalidSlotNumberException {
        return updateRecord(new RID(mid.pageNo, mid.slotNo), map.getMapByteArray());
    }

    public MID firstMap() throws IOException {
        MID mid = new MID();
        RID rid = new RID();
        rid = firstRecord();
        if (rid == null)
            return null;
        mid.pageNo = rid.pageNo;
        mid.slotNo = rid.slotNo;
        return mid;
    }

    public MID nextMap(MID curMid) throws IOException, InvalidSlotNumberException {
        MID mid = new MID();
        RID paramrid = new RID();

        int slotCnt = Convert.getShortValue(SLOT_CNT, data);
        if (curMid.slotNo / 3 >= slotCnt) {
            throw new InvalidSlotNumberException();
        }

        paramrid.pageNo = curMid.pageNo;
        paramrid.slotNo = curMid.slotNo;
        RID rid = new RID();
        rid = nextRecord(paramrid);
        if (rid == null)
            return null;
        mid.pageNo = rid.pageNo;
        mid.slotNo = rid.slotNo;
        return mid;
    }

}