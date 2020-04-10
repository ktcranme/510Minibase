package storage;

import BigT.Mapview;
import diskmgr.Page;
import global.Convert;
import global.MID;
import global.PageId;
import global.RID;
import heap.HFPage;
import heap.InvalidSlotNumberException;
import heap.InvalidUpdateException;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.stream.Collectors;

public class SmallMapPage extends HFPage implements Mapview {
    String ignoredLabel;
    Integer ignoredPos;

    public SmallMapPage(Page page, String ignoredLabel, Integer ignoredPos) {
        super(page);
        this.ignoredPos = ignoredPos;
        this.ignoredLabel = ignoredLabel;
    }

    public SmallMapPage(String ignoredLabel, Integer ignoredPos) {
        super();
        this.ignoredPos = ignoredPos;
        this.ignoredLabel = ignoredLabel;
    }

    public void init(PageId pageNo, Page apage, String ignoredLabel, Integer ignoredPos) throws IOException {
        super.init(pageNo, apage);
        this.ignoredLabel = ignoredLabel;
        this.ignoredPos = ignoredPos;
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
    public void sort() throws IOException {
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
            String key = map.getValue();
            int j = i - 1;

            while (j >= 0 && new SmallMap(data, start - (j + 1) * SmallMap.map_size).getValue().compareTo(key) > 0) {
                byte[] temp = new byte[SmallMap.map_size];
                short slotOffsetDest = (short) (start - (j + 2) * SmallMap.map_size);
                short slotOffsetSrc  = (short) (start - (j + 1) * SmallMap.map_size);

                if (!offsetToSlot.containsKey(slotOffsetSrc) || offsetToSlot.get(slotOffsetSrc) == -1)
                    continue;

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

    public String getMaxVal() throws IOException, InvalidSlotNumberException {
        String maxVal = "";
        MID mid = firstMap();
        SmallMap map = null;
        while (mid != null) {
            map = new SmallMap(getRecord(new RID(mid.pageNo, mid.slotNo)), 0);
            if (map.getValue().compareTo(maxVal) > 0) {
                maxVal = map.getValue();
            }

            mid = nextMap(mid);
        }

        return maxVal;
    }

    public String getMinVal() throws IOException, InvalidSlotNumberException {
        String minVal = "";
        MID mid = firstMap();

        if (mid == null)
            return minVal;

        SmallMap map = null;
        map = new SmallMap(getRecord(new RID(mid.pageNo, mid.slotNo)), 0);
        minVal = map.getValue();
        mid = nextMap(mid);

        while (mid != null) {
            map = new SmallMap(getRecord(new RID(mid.pageNo, mid.slotNo)), 0);
            if (map.getValue().compareTo(minVal) < 0) {
                minVal = map.getValue();
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

    public void migrateHalf(SmallMapPage page) throws IOException, InvalidSlotNumberException {
        HashMap<RID, Integer> hmap = new HashMap<RID, Integer>();
        List<RID> rids = new ArrayList<>();
        MID mid = firstMap();
        SmallMap map = null;
        while (mid != null) {
            RID rid = new RID(mid.pageNo, mid.slotNo);
            map = new SmallMap(getRecord(rid), 0);
            hmap.put(rid, Integer.parseInt(map.getValue()));
            rids.add(rid);
            mid = nextMap(mid);
        }

        List<RID> ridList = rids.stream().sorted((rid1, rid2) -> {
            return hmap.get(rid2).compareTo(hmap.get(rid1));
        }).limit(rids.size() / 2).collect(Collectors.toList());

        for (RID rid : ridList) {
            page.insertRecord(getRecord(rid));
            deleteRecord(rid);
        }
    }

    public BigT.Map getMap(MID mid) throws InvalidSlotNumberException, IOException {
        byte[] rec = getRecord(new RID(mid.pageNo, mid.slotNo));
        return (new SmallMap(rec, 0)).toMap(this.ignoredLabel, this.ignoredPos);
    }

    public MID updateMap(MID mid, BigT.Map map) throws IOException, InvalidSlotNumberException, InvalidUpdateException {
        if (updateRecord(
                new RID(mid.pageNo, mid.slotNo),
               (new SmallMap(map, this.ignoredPos)).getMapByteArray())
        ) return mid;
        return null;
    }

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