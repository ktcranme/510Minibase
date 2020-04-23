package storage;

import BigT.Mapview;
import diskmgr.Page;
import global.Convert;
import global.MID;
import global.PageId;
import global.RID;
import heap.HFPage;
import heap.InvalidSlotNumberException;
import BigT.Map;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.stream.Collectors;

public class SmallMapPage extends HFPage {
    Integer pkLength;

    public int DPFIXED = 4 * 2 + 3 * 4;
    public static int IGNORED_LABEL = 20;
    
    public SmallMapPage(Page page, Integer pkLength) {
        super(page);
        this.pkLength = pkLength;
        this.DPFIXED += pkLength;
    }

    public SmallMapPage(Integer pkLength) {
        super();
        this.pkLength = pkLength;
        this.DPFIXED += pkLength;
    }

    public String getPrimaryKey() throws IOException {
        return Convert.getStrValue(IGNORED_LABEL, data, this.pkLength);
    }

    @Override
    public RID insertRecord(byte[] record) throws IOException {
        RID rid = new RID();

        int recLen = record.length;
        int spaceNeeded = recLen + SIZE_OF_SLOT;

        // Start by checking if sufficient space exists.
        // This is an upper bound check. May not actually need a slot
        // if we can find an empty one.

        short freeSpace = Convert.getShortValue(FREE_SPACE, data);
//        if (spaceNeeded > freeSpace) {
//            return null;
//
//        } else {

            // look for an empty slot
            short slotCnt = Convert.getShortValue(SLOT_CNT, data);
            int i;
            short length;
            for (i = 0; i < slotCnt; i++) {
                length = getSlotLength(i);
                if (length == EMPTY_SLOT)
                    break;
            }

            if (i == slotCnt) // use a new slot
            {
                if (spaceNeeded > freeSpace)
                    return null;
                // adjust free space
                freeSpace -= spaceNeeded;
                Convert.setShortValue(freeSpace, FREE_SPACE, data);

                slotCnt++;
                Convert.setShortValue(slotCnt, SLOT_CNT, data);

            } else {
                if (recLen > freeSpace)
                    return null;
                // reusing an existing slot
                freeSpace -= recLen;
                Convert.setShortValue(freeSpace, FREE_SPACE, data);
            }

            short usedPtr = Convert.getShortValue(USED_PTR, data);
            usedPtr -= recLen; // adjust usedPtr
            Convert.setShortValue(usedPtr, USED_PTR, data);

            // insert the slot info onto the data page
            setSlot(i, recLen, usedPtr);

            // insert data onto the data page
            System.arraycopy(record, 0, data, usedPtr, recLen);
            curPage.pid = Convert.getIntValue(CUR_PAGE, data);
            rid.pageNo.pid = curPage.pid;
            rid.slotNo = i;
            return rid;
//        }
    }

    @Override
    public void deleteRecord(RID rid) throws IOException, InvalidSlotNumberException {
        int slotNo = rid.slotNo;
        short recLen = getSlotLength(slotNo);
        short slotCnt = Convert.getShortValue(SLOT_CNT, data);

        // first check if the record being deleted is actually valid
        if ((slotNo >= 0) && (slotNo < slotCnt) && (recLen > 0)) {
            // The records always need to be compacted, as they are
            // not necessarily stored on the page in the order that
            // they are listed in the slot index.

            // offset of record being deleted
            int offset = getSlotOffset(slotNo);
            short usedPtr = Convert.getShortValue(USED_PTR, data);
            int newSpot = usedPtr + recLen;
            int size = offset - usedPtr;

            // shift bytes to the right
            System.arraycopy(data, usedPtr, data, newSpot, size);

            // now need to adjust offsets of all valid slots that refer
            // to the left of the record being removed. (by the size of the hole)

            int i, n, chkoffset;
            for (i = 0, n = this.DPFIXED; i < slotCnt; n += SIZE_OF_SLOT, i++) {
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
    }

    @Override
    public void init(PageId pageNo, Page apage) throws IOException {
        throw new IOException("Dont call this!");
    }

    @Override
    public short getSlotOffset(int slotno) throws IOException {
        int position = this.DPFIXED + slotno * SIZE_OF_SLOT;
        short val = Convert.getShortValue(position + 2, data);
        return val;
    }

    @Override
    public short getSlotLength(int slotno) throws IOException {
        int position = this.DPFIXED + slotno * SIZE_OF_SLOT;
        short val = Convert.getShortValue(position, data);
        return val;
    }

    @Override
    public void setSlot(int slotno, int length, int offset) throws IOException {
        int position = this.DPFIXED + slotno * SIZE_OF_SLOT;
        Convert.setShortValue((short) length, position, data);
        Convert.setShortValue((short) offset, position + 2, data);
    }

    public void init(PageId pageNo, Page apage, Integer pkLength, String primary) throws IOException {
        data = apage.getpage();
        Convert.setShortValue((short) 0, SLOT_CNT, data);

        this.pkLength = pkLength;
        Convert.setStrValue (primary, IGNORED_LABEL, data);

        curPage.pid = pageNo.pid;
        Convert.setIntValue(curPage.pid, CUR_PAGE, data);

        Convert.setIntValue(INVALID_PAGE, PREV_PAGE, data);
        Convert.setIntValue(INVALID_PAGE, NEXT_PAGE, data);

        Convert.setShortValue((short) MAX_SPACE, USED_PTR, data);

        Convert.setShortValue((short) (MAX_SPACE - this.DPFIXED), FREE_SPACE, data);
    }

    public boolean isEmpty() throws IOException {
        short usedPtr = Convert.getShortValue(USED_PTR, data);
        return usedPtr == MAX_SPACE;
    }

    public void printSeq() throws IOException {
        short start = MAX_SPACE;
        short slotCnt = Convert.getShortValue(SLOT_CNT, data);

        for (short i = 0; i < slotCnt; i++) {
            SmallMap map = new SmallMap(data, start - (i + 1) * SmallMap.map_size);
            map.print();
        }
    }

    // Expects all the elements to be tightly packed
    public void sort(Integer secondarykey) throws IOException {
        if (secondarykey == null)
            return;

        short start = MAX_SPACE;
        short totalSlots = Convert.getShortValue(SLOT_CNT, data);
        short usedPtr = Convert.getShortValue(USED_PTR, data);
        short slotCnt = (short) ((start - usedPtr) / SmallMap.map_size);

        HashMap<Short, Short> offsetToSlot = new HashMap<>();
        for (short i = 0; i < totalSlots; i++) {
            short length = getSlotLength(i);
            if (length == SmallMap.map_size)
                offsetToSlot.put(getSlotOffset(i), i);
            else {
                offsetToSlot.put(getSlotOffset(i), (short) -1);
            }
        }

        for (short i = 1; i < slotCnt; i++) {
            SmallMap map = new SmallMap(data, start - (i + 1) * SmallMap.map_size);
            String key = map.getKey(secondarykey);
            int j = i - 1;

            while (j >= 0 && new SmallMap(data, start - (j + 1) * SmallMap.map_size).getKey(secondarykey).compareTo(key) > 0) {
                byte[] temp = new byte[SmallMap.map_size];
                short slotOffsetDest = (short) (start - (j + 2) * SmallMap.map_size);
                short slotOffsetSrc  = (short) (start - (j + 1) * SmallMap.map_size);

                if (!offsetToSlot.containsKey(slotOffsetSrc) || offsetToSlot.get(slotOffsetSrc) == -1) {
                    j -= 1;
                    continue;
                }

                if (!offsetToSlot.containsKey(slotOffsetDest) ||!offsetToSlot.containsKey(slotOffsetSrc) ) {
                    System.out.println("TEST");
                }

                short slotDest = offsetToSlot.remove(slotOffsetDest);
                short slotSrc  = offsetToSlot.remove(slotOffsetSrc);

                offsetToSlot.put(slotOffsetSrc, slotDest);
                offsetToSlot.put(slotOffsetDest, slotSrc);

                System.arraycopy(data, slotOffsetDest, temp, 0, SmallMap.map_size);
                System.arraycopy(data, slotOffsetSrc, data, slotOffsetDest, SmallMap.map_size);
                System.arraycopy(temp, 0, data, slotOffsetSrc, SmallMap.map_size);

                setSlot(slotDest, SmallMap.map_size, slotOffsetSrc);
                setSlot(slotSrc, SmallMap.map_size, slotOffsetDest);

                j = j - 1;
            }

            byte[] temp = map.getMapByteArray();
            System.arraycopy(data, start - (j + 2) * SmallMap.map_size, temp, 0, SmallMap.map_size);
        }
    }

    public String getMaxVal(int key) throws IOException, InvalidSlotNumberException {
        String maxVal = "";
        MID mid = firstMap();
        SmallMap map = null;
        while (mid != null) {
            map = new SmallMap(getRecord(new RID(mid.pageNo, mid.slotNo)), 0);
            if (map.getKey(key).compareTo(maxVal) > 0) {
                maxVal = map.getKey(key);
            }

            mid = nextMap(mid);
        }

        return maxVal;
    }

    public String getMinVal(int key) throws IOException, InvalidSlotNumberException {
        String minVal = "";
        MID mid = firstMap();

        if (mid == null)
            return minVal;

        SmallMap map = null;
        map = new SmallMap(getRecord(new RID(mid.pageNo, mid.slotNo)), 0);
        minVal = map.getKey(key);
        mid = nextMap(mid);

        while (mid != null) {
            map = new SmallMap(getRecord(new RID(mid.pageNo, mid.slotNo)), 0);
            if (map.getKey(key).compareTo(minVal) < 0) {
                minVal = map.getKey(key);
            }
            mid = nextMap(mid);
        }

        return minVal;
    }

    public void printAllValues() throws IOException, InvalidSlotNumberException {
        MID mid = firstMap();
        SmallMap map = null;
        while (mid != null) {
            map = new SmallMap(getRecord(new RID(mid.pageNo, mid.slotNo)), 0);
            map.print();
            mid = nextMap(mid);
        }
    }

    public void migrateAll(SmallMapPage page) {
        System.arraycopy(data, this.DPFIXED, page.data, this.DPFIXED, MAX_SPACE - this.DPFIXED);
    }

    public void migrateHalf(SmallMapPage page, Integer key) throws IOException, InvalidSlotNumberException {
        sort(key);
        short start = MAX_SPACE;
        short usedPtr = Convert.getShortValue(USED_PTR, data);
        int recs = (start - usedPtr) / SmallMap.map_size;
        int lenToBeCopied = (recs / 2) * SmallMap.map_size;
        System.arraycopy(data, usedPtr, page.data, start - lenToBeCopied, lenToBeCopied);

        int slotCnt = Convert.getShortValue(SLOT_CNT, data);
        usedPtr += lenToBeCopied;
        for (int i = 0; i < slotCnt; i++) {
            int offset = getSlotOffset(i);
            if (offset < usedPtr) {
                setSlot(i, EMPTY_SLOT, 0); // mark slot free
                page.setSlot(i, SmallMap.map_size, start - (usedPtr - offset));
            } else {
                page.setSlot(i, EMPTY_SLOT, 0);
            }
        }
        short freeSpace = Convert.getShortValue(FREE_SPACE, data);
        freeSpace += lenToBeCopied;
        Convert.setShortValue(usedPtr, USED_PTR, data);
        Convert.setShortValue(freeSpace, FREE_SPACE, data);

        freeSpace = Convert.getShortValue(FREE_SPACE, page.data);
        freeSpace -= lenToBeCopied + slotCnt * HFPage.SIZE_OF_SLOT;
        Convert.setShortValue(freeSpace, FREE_SPACE, page.data);
        Convert.setShortValue((short) (start - lenToBeCopied), USED_PTR, page.data);
        Convert.setShortValue((short) (slotCnt), SLOT_CNT, page.data);
    }

    public Map getMap(MID mid, Integer primaryKey) throws InvalidSlotNumberException, IOException {
        byte[] rec = getRecord(new RID(mid.pageNo, mid.slotNo));
        return (new SmallMap(rec, 0)).toMap(getPrimaryKey(), primaryKey);
    }

//    public MID updateMap(MID mid, BigT.Map map) throws IOException, InvalidSlotNumberException, InvalidUpdateException {
//        if (updateRecord(
//                new RID(mid.pageNo, mid.slotNo),
//               (new SmallMap(map, this.ignoredPos)).getMapByteArray())
//        ) return mid;
//        return null;
//    }

    public MID firstMap() throws IOException {
        MID mid = new MID();
        RID rid = firstRecord();
        if (rid == null)
            return null;
        mid.pageNo = rid.pageNo;
        mid.slotNo = rid.slotNo;
        return mid;
    }

    public MID firstSorted() throws IOException, InvalidSlotNumberException {
        MID mid = new MID();
        mid.pageNo.pid = Convert.getIntValue(CUR_PAGE, data);
        int slotCnt = Convert.getShortValue(SLOT_CNT, data);
        short start = MAX_SPACE - SmallMap.map_size;

        for (int i = 0; i < slotCnt; i++) {
            if (getSlotOffset(i) == start) {
                mid.slotNo = i;
                return mid;
            }
        }

        throw new InvalidSlotNumberException();
    }

    public MID nextSorted(MID prevMid) throws IOException, InvalidSlotNumberException {
        MID mid = new MID();
        mid.pageNo.pid = Convert.getIntValue(CUR_PAGE, data);
        int slotCnt = Convert.getShortValue(SLOT_CNT, data);
        short curOffset = getSlotOffset(prevMid.slotNo);
        short nextOffset = (short) (curOffset - SmallMap.map_size);

        for (int i = 0; i < slotCnt; i++) {
            if (getSlotOffset(i) == nextOffset) {
                mid.slotNo = i;
                return mid;
            }
        }

//        throw new InvalidSlotNumberException();
        return null;
    }

    public MID nextMap(MID curMid) throws IOException, InvalidSlotNumberException {
        MID mid = new MID();
        RID paramrid = new RID();

        int slotCnt = Convert.getShortValue(SLOT_CNT, data);
        if (curMid.slotNo >= slotCnt) {
            throw new InvalidSlotNumberException();
        }

        paramrid.pageNo = curMid.pageNo;
        paramrid.slotNo = curMid.slotNo;
        RID rid = nextRecord(paramrid);
        if (rid == null)
            return null;
        mid.pageNo = rid.pageNo;
        mid.slotNo = rid.slotNo;
        return mid;
    }

}