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
 * Navigates a Page of Physical Maps and returns Virtual Maps
 */
public class Mapfile extends Heapfile {
    public static final int VERSIONS = 3;

    public Mapfile(String name) throws HFException, HFBufMgrException, HFDiskMgrException, IOException {
        super(name);
    }

    public Stream openStream() throws InvalidMapSizeException, InvalidTupleSizeException, IOException,
            HFBufMgrException, InvalidSlotNumberException {
        Stream newscan = new Stream(this);
        return newscan;
    }

    public MID insertMap(Map m) throws InvalidSlotNumberException, InvalidTupleSizeException,
            SpaceNotAvailableException, HFException, HFBufMgrException, HFDiskMgrException, IOException {
        RID rid = insertRecord(PhysicalMap.getMapByteArray(m));
        return new MID(rid.pageNo, rid.slotNo * VERSIONS);
    }

    public boolean deleteMap(MID mid) throws InvalidSlotNumberException, InvalidTupleSizeException, HFException,
            HFBufMgrException, HFDiskMgrException, Exception {
        return deleteRecord(new RID(mid.pageNo, mid.slotNo / VERSIONS));
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
        paramrid.slotNo = mid.slotNo / VERSIONS;

        status = _findDataPage(paramrid, currentDirPageId, dirPage, currentDataPageId, dataPage, currentDataPageRid);

        if (status != true)
            return status; // record not found

        PhysicalMap amap = dataPage.returnRecord(new RID(mid.pageNo, mid.slotNo / VERSIONS)).toPhysicalMap();

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
        return getRecord(new RID(mid.pageNo, mid.slotNo / VERSIONS)).toMap(mid.slotNo % VERSIONS);
    }

    public int getMapCnt() throws InvalidSlotNumberException, InvalidTupleSizeException, HFDiskMgrException,
            HFBufMgrException, IOException {
        // TODO: How do we find the actual number of maps in file? Is it just row/col
        // combo or does it include all the versions?
        return getRecCnt();
    }
}