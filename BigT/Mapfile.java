package BigT;

import java.io.*;
import BigT.*;
import diskmgr.*;
import bufmgr.*;
import global.*;
import heap.DataPageInfo;
import heap.Dirpage;
import heap.HFBufMgrException;
import heap.HFDiskMgrException;
import heap.HFException;
import heap.Heapfile;
import heap.InvalidSlotNumberException;
import heap.InvalidTupleSizeException;
import heap.InvalidUpdateException;
import heap.SpaceNotAvailableException;

public class Mapfile extends Heapfile implements Bigtablefile {

    public Mapfile(String name) throws HFException, HFBufMgrException, HFDiskMgrException, IOException {
        super(name);
    }

    public MapPage getNewDataPage() {
        return new MapPage();
    }

    public MapPage getNewDataPage(Page page) {
        return new MapPage(page);
    }

    public MapPage getNewDataPage(Page page, PageId pid) throws IOException {
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

    public MID updateMap(MID rid, Map newtuple) throws InvalidSlotNumberException, InvalidUpdateException,
            InvalidTupleSizeException, HFException, HFDiskMgrException, HFBufMgrException, Exception {

        boolean status;
        Dirpage dirPage = new Dirpage();
        PageId currentDirPageId = new PageId();
        MapPage dataPage = getNewDataPage();
        PageId currentDataPageId = new PageId();
        RID currentDataPageRid = new RID();

        status = _findDataPage(new RID(rid.pageNo, rid.slotNo / 3), currentDirPageId, dirPage, currentDataPageId, dataPage, currentDataPageRid);

        if (status != true)
            throw new InvalidSlotNumberException();

        MID updatedRecord = dataPage.updateMap(rid, newtuple);
        
        if (!updatedRecord.isReused) {
            DataPageInfo dpinfo_ondirpage = dirPage.returnDatapageInfo(currentDataPageRid);
            dpinfo_ondirpage.recct++;
            dpinfo_ondirpage.flushToTuple();
        }

        unpinPage(currentDataPageId, true /* = DIRTY */);
        unpinPage(currentDirPageId, !updatedRecord.isReused /* undirty ? */);

        return updatedRecord;
    }

    public boolean deleteMap(MID rid) throws InvalidSlotNumberException, InvalidTupleSizeException, HFException,
            HFBufMgrException, HFDiskMgrException, Exception {
		boolean status;
		Dirpage currentDirPage = new Dirpage();
		PageId currentDirPageId = new PageId();
		MapPage currentDataPage = getNewDataPage();
		PageId currentDataPageId = new PageId();
		RID currentDataPageRid = new RID();

		status = _findDataPage(new RID(rid.pageNo, rid.slotNo / 3), currentDirPageId, currentDirPage, currentDataPageId, currentDataPage,
				currentDataPageRid);

		if (status != true)
			return status; // record not found

		// ASSERTIONS:
		// - currentDirPage, currentDirPageId valid and pinned
		// - currentDataPage, currentDataPageid valid and pinned

		// get datapageinfo from the current directory page:
		DataPageInfo pdpinfo = currentDirPage.returnDatapageInfo(currentDataPageRid);

		// delete the record on the datapage
		int versions = currentDataPage.deleteMap(rid);

		pdpinfo.recct -= versions;
		pdpinfo.flushToTuple(); // Write to the buffer pool
		if (pdpinfo.recct >= 1) {
			// more records remain on datapage so it still hangs around.
			// we just need to modify its directory entry

			pdpinfo.availspace = currentDataPage.available_space();
			pdpinfo.flushToTuple();
			unpinPage(currentDataPageId, true /* = DIRTY */);

			unpinPage(currentDirPageId, true /* = DIRTY */);

		} else {
			// the record is already deleted:
			// we're removing the last record on datapage so free datapage
			// also, free the directory page if
			// a) it's not the first directory page, and
			// b) we've removed the last DataPageInfo record on it.

			// delete empty datapage: (does it get unpinned automatically? -NO, Ranjani)
			unpinPage(currentDataPageId, false /* undirty */);

			freePage(currentDataPageId);

			// delete corresponding DataPageInfo-entry on the directory page:
			// currentDataPageRid points to datapage (from for loop above)

			currentDirPage.deleteRecord(currentDataPageRid);

			// ASSERTIONS:
			// - currentDataPage, currentDataPageId invalid
			// - empty datapage unpinned and deleted

			// now check whether the directory page is empty:

			currentDataPageRid = currentDirPage.firstRecord();

			// st == OK: we still found a datapageinfo record on this directory page
			PageId pageId;
			pageId = currentDirPage.getPrevPage();
			if ((currentDataPageRid == null) && (pageId.pid != INVALID_PAGE)) {
				// the directory-page is not the first directory page and it is empty:
				// delete it

				// point previous page around deleted page:

				Dirpage prevDirPage = new Dirpage();
				pinPage(pageId, prevDirPage, false);

				pageId = currentDirPage.getNextPage();
				prevDirPage.setNextPage(pageId);
				pageId = currentDirPage.getPrevPage();
				unpinPage(pageId, true /* = DIRTY */);

				// set prevPage-pointer of next Page
				pageId = currentDirPage.getNextPage();
				if (pageId.pid != INVALID_PAGE) {
					Dirpage nextDirPage = new Dirpage();
					pageId = currentDirPage.getNextPage();
					pinPage(pageId, nextDirPage, false);

					// nextDirPage.openHFpage(apage);

					pageId = currentDirPage.getPrevPage();
					nextDirPage.setPrevPage(pageId);
					pageId = currentDirPage.getNextPage();
					unpinPage(pageId, true /* = DIRTY */);

				}

				// delete empty directory page: (automatically unpinned?)
				unpinPage(currentDirPageId, false/* undirty */);
				freePage(currentDirPageId);

			} else {
				// either (the directory page has at least one more datapagerecord
				// entry) or (it is the first directory page):
				// in both cases we do not delete it, but we have to unpin it:

				unpinPage(currentDirPageId, true /* == DIRTY */);

			}
		}
		return true;
    }

    public MID insertMap(Map tuple) throws InvalidSlotNumberException, InvalidTupleSizeException,
            SpaceNotAvailableException, HFException, HFBufMgrException, HFDiskMgrException, IOException {
        RID rid = insertRecord(PhysicalMap.getMapByteArray(tuple));
        return new MID(rid.pageNo, rid.slotNo * 3);
    }

    public Stream openStream() throws InvalidMapSizeException, InvalidTupleSizeException, HFBufMgrException,
            InvalidSlotNumberException, IOException {
        return new Stream(this);
    }
}