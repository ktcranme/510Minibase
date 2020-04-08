package storage;

import BigT.*;
import diskmgr.Page;
import global.MID;
import global.PageId;
import global.RID;
import heap.*;

import java.io.IOException;

public class SmallMapFile extends Heapfile implements Bigtablefile {
    String ignoredLabel;
    Integer ignoredPos;

    public SmallMapFile(String name, String ignoredLabel, Integer ignoredPos) throws HFException, HFBufMgrException, HFDiskMgrException, IOException {
        super(name);
        this.ignoredLabel = ignoredLabel;
        this.ignoredPos = ignoredPos;
    }

    public SmallMapPage getNewDataPage() {
        return new SmallMapPage(this.ignoredLabel, this.ignoredPos);
    }

    public SmallMapPage getNewDataPage(Page page) {
        return new SmallMapPage(page, this.ignoredLabel, this.ignoredPos);
    }

    public SmallMapPage getNewDataPage(Page page, PageId pid) throws IOException {
        SmallMapPage hfp = new SmallMapPage(this.ignoredLabel, this.ignoredPos);
        hfp.init(pid, page, this.ignoredLabel, this.ignoredPos);
        return hfp;
    }

    public int getMapCnt() throws InvalidSlotNumberException, InvalidTupleSizeException, HFDiskMgrException,
            HFBufMgrException, IOException {
        return getRecCnt();
    }

    public Map getMap(MID rid) throws InvalidSlotNumberException, InvalidTupleSizeException, HFException,
            HFDiskMgrException, HFBufMgrException, Exception {
        byte[] rec = getRecord(new RID(rid.pageNo, rid.slotNo));
        return new SmallMap(rec, 0).toMap(this.ignoredLabel, this.ignoredPos);
    }

    public MID updateMap(MID rid, Map newtuple) throws InvalidSlotNumberException, InvalidUpdateException,
            InvalidTupleSizeException, HFException, HFDiskMgrException, HFBufMgrException, Exception {
        if (updateRecord(new RID(rid.pageNo, rid.slotNo), new SmallMap(newtuple, this.ignoredPos).getMapByteArray())) {
            return rid;
        }
        return null;
    }

    public boolean deleteMap(MID rid) throws InvalidSlotNumberException, InvalidTupleSizeException, HFException,
            HFBufMgrException, HFDiskMgrException, Exception {
        return deleteRecord(new RID(rid.pageNo, rid.slotNo));
    }

    public MID insertMap(Map tuple) throws InvalidSlotNumberException, InvalidTupleSizeException,
            SpaceNotAvailableException, HFException, HFBufMgrException, HFDiskMgrException, IOException {
        RID rid = insertRecord(new SmallMap(tuple, this.ignoredPos).getMapByteArray());
        return new MID(rid.pageNo, rid.slotNo);
    }

    @Override
    public Stream openStream() throws InvalidMapSizeException, InvalidTupleSizeException, HFBufMgrException, InvalidSlotNumberException, IOException {
        return new Stream(this);
    }
}
