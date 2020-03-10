package BigT;

import java.io.IOException;

import global.Convert;
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
                PhysicalMap m = returnRecord(new RID(curMid.pageNo, curMid.slotNo / Mapfile.VERSIONS)).toPhysicalMap();
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

    public Map getMap(MID mid) throws InvalidSlotNumberException, IOException {
        PhysicalMap map = returnRecord(new RID(mid.pageNo, mid.slotNo / Mapfile.VERSIONS)).toPhysicalMap();
        return map.toMap(mid.slotNo % Mapfile.VERSIONS);
    }

    public int deleteMap(RID rid) throws IOException, InvalidSlotNumberException {
        int slotNo = rid.slotNo;
        short recLen = getSlotLength(slotNo);
        slotCnt = Convert.getShortValue(SLOT_CNT, data);
        int versions = 1;

        // first check if the record being deleted is actually valid
        if ((slotNo >= 0) && (slotNo < slotCnt) && (recLen > 0)) {
            // The records always need to be compacted, as they are
            // not necessarily stored on the page in the order that
            // they are listed in the slot index.

            // offset of record being deleted
            int offset = getSlotOffset(slotNo);
            usedPtr = Convert.getShortValue(USED_PTR, data);
            int newSpot = usedPtr + recLen;
            int size = offset - usedPtr;

            PhysicalMap amap = new PhysicalMap(data, offset);
            versions = amap.versionCount();

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
            freeSpace = Convert.getShortValue(FREE_SPACE, data);
            freeSpace += recLen;
            Convert.setShortValue(freeSpace, FREE_SPACE, data);

            setSlot(slotNo, EMPTY_SLOT, 0); // mark slot free
        } else {
            throw new InvalidSlotNumberException(null, "HEAPFILE: INVALID_SLOTNO");
        }

        return versions;
    }
}