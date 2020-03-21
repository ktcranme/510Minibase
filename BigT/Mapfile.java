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
import heap.HFPage;
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
		return this.updateMap(rid, newtuple, null);
	}

	public MID insertOrUpdateMap(MID rid, Map newtuple, Map deletedMap) {

		return null;
	}

    public MID updateMap(MID rid, Map newtuple, Map deletedMap) throws InvalidSlotNumberException, InvalidUpdateException,
            InvalidTupleSizeException, HFException, HFDiskMgrException, HFBufMgrException, Exception {

        boolean status;
        Dirpage dirPage = new Dirpage();
        PageId currentDirPageId = new PageId();
        MapPage dataPage = getNewDataPage();
        PageId currentDataPageId = new PageId();
		RID currentDataPageRid = new RID();
		long timeStart;

		timeStart = System.currentTimeMillis();
        status = _findDataPage(new RID(rid.pageNo, rid.slotNo / 3), currentDirPageId, dirPage, currentDataPageId, dataPage, currentDataPageRid);
		System.out.println("Datapage Find Time taken: "+(System.currentTimeMillis()-timeStart));
		
		if (status != true)
            throw new InvalidSlotNumberException();

		timeStart = System.currentTimeMillis();
		MID updatedRecord = dataPage.updateMap(rid, newtuple, deletedMap);
		System.out.println("Update on Datapage Time taken: "+(System.currentTimeMillis()-timeStart));

        
        if (updatedRecord != null && !updatedRecord.isReused) {
            DataPageInfo dpinfo_ondirpage = dirPage.returnDatapageInfo(currentDataPageRid);
            dpinfo_ondirpage.recct++;
            dpinfo_ondirpage.flushToTuple();
        }

		timeStart = System.currentTimeMillis();
        unpinPage(currentDataPageId, true /* = DIRTY */);
        unpinPage(currentDirPageId, updatedRecord != null && !updatedRecord.isReused /* undirty ? */);
		System.out.println("Flushing datapage and dirpage Time taken: "+(System.currentTimeMillis()-timeStart));

        return updatedRecord;
    }

	public MapPage naiveSearch(Map map) throws InvalidTupleSizeException, InvalidSlotNumberException, IOException,
			Exception {
		PageId currentDirPageId = new PageId(_firstDirPageId.pid);

		Dirpage currentDirPage = new Dirpage();
		MapPage currentDataPage = getNewDataPage();
		RID currentDataPageRid = new RID();
		PageId nextDirPageId = new PageId();

		PageId possibleInsertLoc = null;
		RID possibleInsertLocRid = null;
		PageId possibleInsertDir = null;
		// datapageId is stored in dpinfo.pageId

		pinPage(currentDirPageId, currentDirPage, false/* read disk */);

		while (currentDirPageId.pid != INVALID_PAGE) {// Start While01
														// ASSERTIONS:
														// currentDirPage, currentDirPageId valid and pinned and Locked.
			System.out.println("Looking at DIRECTORY: " + currentDirPageId.pid + ", has space: " + currentDirPage.available_space());
			if (possibleInsertDir == null && currentDirPage.available_space() >= DataPageInfo.size) {
				// If we dont find a datapage to insert into and we have
				// not seen the row col combination, we need to create a new
				// datapage in here
				possibleInsertDir = new PageId(currentDirPageId.pid);
				System.out.println("POSSIBLE dir insert location: " + currentDirPageId.pid + ", has space: " + currentDirPage.available_space());
			}
			for (currentDataPageRid = currentDirPage
					.firstRecord(); currentDataPageRid != null; currentDataPageRid = currentDirPage
							.nextRecord(currentDataPageRid)) {
				DataPageInfo dpinfo = null;
				try {
					dpinfo = currentDirPage.returnDatapageInfo(currentDataPageRid);
					System.out.println("Looking at DATAPAGE: " + dpinfo.pageId + ", has space: " + dpinfo.availspace);

					if (possibleInsertLoc == null && dpinfo.availspace >= Map.map_size + HFPage.SIZE_OF_SLOT) {
						// If we dont find the row col combination, we need to insert
						// here
						possibleInsertLoc = new PageId(dpinfo.pageId.pid);
						possibleInsertDir = new PageId(currentDirPageId.pid);
						possibleInsertLocRid = new RID(currentDataPageRid.pageNo, currentDataPageRid.slotNo);
						System.out.println("POSSIBLE datapage insert location: " + dpinfo.pageId);
					}
				} catch (InvalidSlotNumberException e) {
					return null;
				}

				try {
					pinPage(dpinfo.pageId, currentDataPage, false/* Rddisk */);

					// check error;need unpin currentDirPage
				} catch (Exception e) {
					unpinPage(currentDirPageId, false/* undirty */);
					throw e;
				}

				// ASSERTIONS:
				// - currentDataPage, currentDataPageRid, dpinfo valid
				// - currentDataPage pinned

				RID rid = currentDataPage.firstRecord();
				while (rid != null) {
					PhysicalMap pm = new PhysicalMap(currentDataPage.data, currentDataPage.returnRecord(rid));
					pm.print();
					if (pm.getRowLabel().equals(map.getRowLabel()) && pm.getColumnLabel().equals(map.getColumnLabel())) {
						System.out.println("UPDATING");
						int verCnt = pm.getVersionCount();
						int ver = pm.updateMap(map.getTimeStamp(), map.getValue());

						if (ver == -1)
							return null;

						if (verCnt < 3) {
							dpinfo.recct++;
							dpinfo.availspace = currentDataPage.available_space();
							dpinfo.flushToTuple();
						}

						unpinPage(currentDirPageId, verCnt < 3);
						unpinPage(dpinfo.pageId, true);
						
						return currentDataPage;
					}
					rid = currentDataPage.nextRecord(rid);
				}


				// Dont need this anymore
				unpinPage(dpinfo.pageId, false/* Rddisk */);
			}

			// if we would have found the correct datapage on the current
			// directory page we would have already returned.
			// therefore:
			// read in next directory page:

			nextDirPageId = currentDirPage.getNextPage();
			if (nextDirPageId.pid == INVALID_PAGE) {
				System.out.println("REACHED THE END! CREATING A NEW DIRPAGE!");

				Page pageinbuffer = new Page();
				nextDirPageId = newPage(pageinbuffer, 1);
				// need check error!
				if (nextDirPageId == null)
					throw new HFException(null, "can't new pae");
		
				Dirpage nextDirPage = new Dirpage();
				// initialize new directory page
				nextDirPage.init(nextDirPageId, pageinbuffer);
				PageId temppid = new PageId(INVALID_PAGE);
				nextDirPage.setNextPage(temppid);
				nextDirPage.setPrevPage(currentDirPageId);
		
				// update current directory page and unpin it
				// currentDirPage is already locked in the Exclusive mode
				currentDirPage.setNextPage(nextDirPageId);
				unpinPage(currentDirPageId, true/* dirty */);
		
				currentDirPage.init(nextDirPageId, nextDirPage);

				break;
			}
			try {
				unpinPage(currentDirPageId, false /* undirty */);
			} catch (Exception e) {
				throw new HFException(e, "heapfile,_find,unpinpage failed");
			}

			currentDirPageId.pid = nextDirPageId.pid;
			pinPage(currentDirPageId, currentDirPage, false/* Rdisk */);
		} // end of While01
		// checked all dir pages and all data pages; user record not found:(


		// We are at the end!
		// If we have possible insert datapage, insert there
		// If we have possible insert dirtpage, add a datapage
		// and then insert
		// If both possible insert loc and insert dir is empty,
		// We need to add a new dirpage, add a datapage
		// then insert the new map here

		System.out.println("We are in the endgame now");
		DataPageInfo dpinfo = null;
		if (possibleInsertLoc != null) {
			System.out.println("INSERTING BACK INTO DATAPAGE");

			pinPage(possibleInsertDir, currentDirPage, false);
			dpinfo = currentDirPage.returnDatapageInfo(possibleInsertLocRid);
			pinPage(possibleInsertLoc, currentDataPage, false);
			// Datapage pinned. Next insert and flush dirpage and datapage
		} else if (possibleInsertDir != null) {
			System.out.println("INSERTING BACK INTO DIRPAGE");
			pinPage(possibleInsertDir, currentDirPage, false);
			// Add a new datapage to the possibleInsertDir
			dpinfo = new DataPageInfo();
			currentDataPageRid = new RID();
			currentDataPage = new MapPage(addNewPageToDir(currentDirPage, dpinfo, currentDataPageRid));	
			// Datapage and dirpage pinned. Next insert and flush dirpage and datapage
		} else {
			System.out.println("INSERTING AT THE VERY END");
			// Add a new datapage to the currentdirpage
			dpinfo = new DataPageInfo();
			currentDataPageRid = new RID();
			currentDataPage = new MapPage(addNewPageToDir(currentDirPage, dpinfo, currentDataPageRid));	
			// Dirpage is already created in the while loop before breaking
			// Datapage and dirpage pinned. Next insert and flush dirpage and datapage
		}

		currentDataPage.insertRecord(PhysicalMap.getMapByteArray(map));

		unpinPage(currentDataPage.getCurPage(), true/* dirty */);

		dpinfo.recct++;
		dpinfo.availspace = currentDataPage.available_space();
		dpinfo.flushToTuple();
		unpinPage(currentDirPage.getCurPage(), true/* dirty */);

		return null;
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