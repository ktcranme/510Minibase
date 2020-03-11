/* File HFPage.java */

package BigT;

import java.io.*;
import java.lang.*;
import BigT.*;

import global.*;
import heap.HFPage;
import heap.InvalidSlotNumberException;
import diskmgr.*;

import BigT.Map;

public class MapPage extends HFPage implements Mapview {
    public MapPage(Page page) {
        super(page);
    }

    public MapPage() {
        super();
    }

    public Map getMap(MID mid) throws InvalidSlotNumberException, IOException {
        byte[] rec = getRecord(new RID(mid.pageNo, mid.slotNo / 3));
        return PhysicalMap.physicalMapToMap(rec, mid.slotNo % 3);
    }

    public boolean updateMap(MID mid, Map map) throws IOException, InvalidSlotNumberException {
        short recLen;
        short offset;
        PageId pageNo = new PageId();
        pageNo.pid = mid.pageNo.pid;

        curPage.pid = Convert.getIntValue(CUR_PAGE, data);
        int slotNo = mid.slotNo / 3;

        // length of record being returned
        recLen = getSlotLength(slotNo);
        int slotCnt = Convert.getShortValue(SLOT_CNT, data);

        if ((slotNo >= 0) && (slotNo < slotCnt) && (recLen > 0) && (pageNo.pid == curPage.pid)) {

            offset = getSlotOffset(slotNo);

            PhysicalMap pmap = new PhysicalMap(data, offset);
            pmap.updateMap(map.getTimeStamp(), map.getValue());

            return true;
        }

        else {
            throw new InvalidSlotNumberException(null, "HEAPFILE: INVALID_SLOTNO");
        }
    }

    public MID firstMap() throws IOException {
        MID mid = new MID();
        RID rid = new RID();
        rid = firstRecord();
        if (rid == null)
            return null;
        mid.pageNo = rid.pageNo;
        mid.slotNo = rid.slotNo * 3;
        return mid;
    }

    public MID nextMap(MID curMid) throws IOException, InvalidSlotNumberException {
        MID mid = new MID();
        RID paramrid = new RID();

        int slotCnt = Convert.getShortValue(SLOT_CNT, data);
        if (curMid.slotNo / 3 >= slotCnt) {
            throw new InvalidSlotNumberException();
        }

        if (curMid.slotNo % 3 < 2) {
            int offset = getSlotOffset(curMid.slotNo / 3);
            PhysicalMap m = new PhysicalMap(data, offset);
            // System.out.println("ACTUAL RECORD: " + m.getRowLabel() + ", " +
            // m.getColumnLabel() + ", " + m.getFirstVer() + ", " + m.getSecondVer() + ", "
            // + m.getThirdVer());
            String val = m.getVersion(curMid.slotNo % 3 + 1);
            if (!val.isEmpty()) {
                mid.pageNo = curMid.pageNo;
                mid.slotNo = curMid.slotNo + 1;
                return mid;
            }
        }

        paramrid.pageNo = curMid.pageNo;
        paramrid.slotNo = curMid.slotNo / 3;
        RID rid = new RID();
        rid = nextRecord(paramrid);
        if (rid == null)
            return null;
        mid.pageNo = rid.pageNo;
        mid.slotNo = rid.slotNo * 3;
        return mid;
    }

}