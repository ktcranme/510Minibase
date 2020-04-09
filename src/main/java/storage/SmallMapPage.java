package storage;

import BigT.Map;
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
import java.util.stream.Stream;

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