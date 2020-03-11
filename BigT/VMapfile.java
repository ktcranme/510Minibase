package BigT;

import java.io.IOException;

import global.MID;
import global.PageId;
import global.RID;
import heap.HFBufMgrException;
import heap.HFDiskMgrException;
import heap.HFException;
import heap.HFPage;
import heap.Heapfile;
import heap.InvalidSlotNumberException;
import heap.InvalidTupleSizeException;
import heap.SpaceNotAvailableException;

/**
 * Virtual Map write and read into Page
 */
public class VMapfile extends Heapfile {
    public VMapfile(String name) throws HFException, HFBufMgrException, HFDiskMgrException, IOException {
        super(name);
    }

    public MID insertMap(Map m) throws InvalidSlotNumberException, InvalidTupleSizeException,
            SpaceNotAvailableException, HFException, HFBufMgrException, HFDiskMgrException, IOException {
        RID rid = insertRecord(PhysicalMap.getMapByteArray(m));
        return new MID(rid.pageNo, rid.slotNo);
    }

    public boolean deleteMap(MID mid) throws InvalidSlotNumberException, InvalidTupleSizeException, HFException,
            HFBufMgrException, HFDiskMgrException, Exception {
        return deleteRecord(new RID(mid.pageNo, mid.slotNo));
    }

    public boolean updateMap(MID mid, Map newmap)
            throws InvalidTupleSizeException, HFException, HFDiskMgrException, Exception {
        boolean status;
        HFPage dirPage = new HFPage();
        PageId currentDirPageId = new PageId();
        HFPage dataPage = new HFPage();
        PageId currentDataPageId = new PageId();
        RID currentDataPageRid = new RID();

        // convert mid to rid, for proper working of _findDatapage()
        RID paramrid = new RID();
        paramrid.pageNo = mid.pageNo;
        paramrid.slotNo = mid.slotNo;

        status = _findDataPage(paramrid, currentDirPageId, dirPage, currentDataPageId, dataPage, currentDataPageRid);

        if (status != true)
            return status; // record not found

        PhysicalMap amap = dataPage.returnRecord(new RID(mid.pageNo, mid.slotNo)).toPhysicalMap();

        try {
            amap.updateMap(newmap.getTimeStamp(), newmap.getValue());
        } catch (IOException e) {
            System.err.println("Map update failed!");
            throw e;
        }

        unpinPage(currentDataPageId, true /* = DIRTY */);
        unpinPage(currentDirPageId, false /* undirty */);

        return true;
    }

    public Map getMap(MID mid) throws InvalidSlotNumberException, InvalidTupleSizeException, HFException,
            HFBufMgrException, HFDiskMgrException, Exception {
        return getRecord(new RID(mid.pageNo, mid.slotNo)).toMap(mid.slotNo);
    }

    public int getMapCnt() throws InvalidSlotNumberException, InvalidTupleSizeException, HFDiskMgrException,
            HFBufMgrException, IOException {
        return getRecCnt();
    }
}