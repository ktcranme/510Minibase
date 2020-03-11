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

public class Mapfile extends Heapfile {

    public Mapfile(String name) throws HFException, HFBufMgrException, HFDiskMgrException, IOException {
        super(name);
    }

    protected MapPage getNewDataPage() {
        return new MapPage();
    }

    protected MapPage getNewDataPage(Page page) {
        return new MapPage(page);
    }

    protected MapPage getNewDataPage(Page page, PageId pid) throws IOException {
        MapPage hfp = new MapPage();
        hfp.init(pid, page);
        return hfp;
    }

    public int getMapCnt() throws InvalidSlotNumberException, InvalidTupleSizeException, HFDiskMgrException,
            HFBufMgrException, IOException {
        return getRecCnt();
    }

    public Map getMap(MID rid) throws InvalidSlotNumberException, InvalidTupleSizeException, HFException,
            HFDiskMgrException, HFBufMgrException, Exception {
        byte[] rec = getRecord(new RID(rid.pageNo, rid.slotNo / 3));
        return PhysicalMap.physicalMapToMap(rec, rid.slotNo % 3);
    }

    public boolean updateMap(MID rid, Map newtuple) throws InvalidSlotNumberException, InvalidUpdateException,
            InvalidTupleSizeException, HFException, HFDiskMgrException, HFBufMgrException, Exception {

        boolean status;
        Dirpage dirPage = new Dirpage();
        PageId currentDirPageId = new PageId();
        MapPage dataPage = getNewDataPage();
        PageId currentDataPageId = new PageId();
        RID currentDataPageRid = new RID();

        status = _findDataPage(new RID(rid.pageNo, rid.slotNo / 3), currentDirPageId, dirPage, currentDataPageId, dataPage, currentDataPageRid);

        if (status != true)
            return status; // record not found

        dataPage.updateMap(rid, newtuple);

        unpinPage(currentDataPageId, true /* = DIRTY */);

        unpinPage(currentDirPageId, false /* undirty */);

        return true;
    }

    public boolean deleteMap(MID rid) throws InvalidSlotNumberException, InvalidTupleSizeException, HFException,
            HFBufMgrException, HFDiskMgrException, Exception {
        return deleteRecord(new RID(rid.pageNo, rid.slotNo / 3));
    }

    public MID insertMap(Map tuple) throws InvalidSlotNumberException, InvalidTupleSizeException,
            SpaceNotAvailableException, HFException, HFBufMgrException, HFDiskMgrException, IOException {
        RID rid = insertRecord(PhysicalMap.getMapByteArray(tuple));
        return new MID(rid.pageNo, rid.slotNo * 3);
    }

    // public Stream openStream() throws InvalidMapSizeException, InvalidTupleSizeException, HFBufMgrException,
    //         InvalidSlotNumberException, IOException {
    //     return new Stream(this);
    // }
}