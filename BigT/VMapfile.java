package BigT;

import java.io.*;
import BigT.*;
import diskmgr.*;
import bufmgr.*;
import global.*;
import heap.Dirpage;
import heap.HFBufMgrException;
import heap.HFDiskMgrException;
import heap.HFException;
import heap.Heapfile;
import heap.InvalidSlotNumberException;
import heap.InvalidTupleSizeException;
import heap.InvalidUpdateException;
import heap.SpaceNotAvailableException;

public class VMapfile extends Heapfile implements Bigtablefile {

    public VMapfile(String name) throws HFException, HFBufMgrException, HFDiskMgrException, IOException {
        super(name);
    }

    public VMapPage getNewDataPage() {
        return new VMapPage();
    }

    public VMapPage getNewDataPage(Page page) {
        return new VMapPage(page);
    }

    public VMapPage getNewDataPage(Page page, PageId pid) throws IOException {
        VMapPage hfp = new VMapPage();
        hfp.init(pid, page);
        return hfp;
    }

    public int getMapCnt() throws InvalidSlotNumberException, InvalidTupleSizeException, HFDiskMgrException,
            HFBufMgrException, IOException {
        return getRecCnt();
    }

    public Map getMap(MID rid) throws InvalidSlotNumberException, InvalidTupleSizeException, HFException,
            HFDiskMgrException, HFBufMgrException, Exception {
        byte[] rec = getRecord(new RID(rid.pageNo, rid.slotNo));
        return new Map(rec, 0);
    }

    public MID updateMap(MID rid, Map newtuple) throws InvalidSlotNumberException, InvalidUpdateException,
            InvalidTupleSizeException, HFException, HFDiskMgrException, HFBufMgrException, Exception {
        if (updateRecord(new RID(rid.pageNo, rid.slotNo), newtuple.getMapByteArray())) {
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
        RID rid = insertRecord(tuple.getMapByteArray());
        return new MID(rid.pageNo, rid.slotNo);
    }

    public Stream openStream() throws InvalidMapSizeException, InvalidTupleSizeException, HFBufMgrException,
            InvalidSlotNumberException, IOException {
        return new Stream(this);
    }
}