/* File HFPage.java */

package BigT;

import java.io.*;
import java.lang.*;
import BigT.*;

import global.*;
import heap.HFPage;
import heap.InvalidSlotNumberException;
import heap.InvalidUpdateException;
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

    public MID updateMap(MID mid, Map map) throws IOException, InvalidSlotNumberException, InvalidUpdateException {
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
            int versions = pmap.getVersionCount();
            int versionUpdated = pmap.updateMap(map.getTimeStamp(), map.getValue());

            if (versionUpdated != -1) {
                MID retMid = new MID(mid.pageNo, mid.slotNo - (mid.slotNo % 3) + versionUpdated);
                retMid.isReused = versions == 3;
                return retMid;
            }

            return null;
        }

        throw new InvalidSlotNumberException(null, "HEAPFILE: INVALID_SLOTNO");
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

	public int deleteMap(MID rid) throws IOException, InvalidSlotNumberException {
        int versions = 1;
        int slotNo = rid.slotNo / 3;
        short recLen = getSlotLength(slotNo);
        int slotCnt = Convert.getShortValue(SLOT_CNT, data);
    
        // first check if the record being deleted is actually valid
        if ((slotNo >= 0) && (slotNo < slotCnt) && (recLen > 0)) {
          // The records always need to be compacted, as they are
          // not necessarily stored on the page in the order that
          // they are listed in the slot index.
    
          // offset of record being deleted
          int offset = getSlotOffset(slotNo);

          PhysicalMap pmap = new PhysicalMap(data, offset);
          versions = pmap.getVersionCount();

          short usedPtr = Convert.getShortValue(USED_PTR, data);
          int newSpot = usedPtr + recLen;
          int size = offset - usedPtr;
    
          // shift bytes to the right
          System.arraycopy(data, usedPtr, data, newSpot, size);
    
          // now need to adjust offsets of all valid slots that refer
          // to the left of the record being removed. (by the size of the hole)
    
          int i, n, chkoffset;
          for (i = 0, n = DPFIXED; i < slotCnt; n += SIZE_OF_SLOT, i++) {
            if ((getSlotLength(i) >= 0)) {
              chkoffset = getSlotOffset(i);
              if (chkoffset < offset) {
                chkoffset += recLen;
                Convert.setShortValue((short) chkoffset, n + 2, data);
              }
            }
          }
    
          // move used Ptr forwar
          usedPtr += recLen;
          Convert.setShortValue(usedPtr, USED_PTR, data);
    
          // increase freespace by size of hole
          short freeSpace = Convert.getShortValue(FREE_SPACE, data);
          freeSpace += recLen;
          Convert.setShortValue(freeSpace, FREE_SPACE, data);
    
          setSlot(slotNo, EMPTY_SLOT, 0); // mark slot free
        } else {
          throw new InvalidSlotNumberException(null, "HEAPFILE: INVALID_SLOTNO");
        }

        return versions;
	}

}