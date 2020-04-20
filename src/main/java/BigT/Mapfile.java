package BigT;

import java.io.*;
import BigT.*;
import diskmgr.*;
import bufmgr.*;
import global.*;
import heap.*;

public class Mapfile extends Heapfile implements Bigtablefile {
	RID insertDatapageRID;
	Dirpage currentDirPageToWrite;
	MapPage currentDataPageToWrite;

    public Mapfile(String name) throws HFException, HFBufMgrException, HFDiskMgrException, IOException {
        super(name);
		insertDatapageRID = null;
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
			HFDiskMgrException, HFBufMgrException, IOException {
        MapPage hfp = new MapPage();
        pinPage(rid.pageNo, hfp, false);
        byte[] rec = hfp.getRecord(new RID(rid.pageNo, rid.slotNo / 3));
        unpinPage(rid.pageNo, false);
        return PhysicalMap.physicalMapToMap(rec, rid.slotNo % 3);
    }

	public MID updateMap(MID rid, Map newtuple) throws InvalidSlotNumberException, InvalidUpdateException,
			InvalidTupleSizeException, HFException, HFDiskMgrException, HFBufMgrException, Exception {
		return this.updateMap(rid, newtuple, null);
	}

    public MID updateMap(MID rid, Map newtuple, Map deletedMap) throws InvalidSlotNumberException, InvalidUpdateException,
            InvalidTupleSizeException, HFException, HFDiskMgrException, HFBufMgrException, Exception {

//        boolean status;
//        Dirpage dirPage = new Dirpage();
//        PageId currentDirPageId = new PageId();
        MapPage dataPage = getNewDataPage();
//        PageId currentDataPageId = new PageId();
//        RID currentDataPageRid = new RID();

		pinPage(rid.pageNo, dataPage, false);

//        status = _findDataPage(new RID(rid.pageNo, rid.slotNo / 3), currentDirPageId, dirPage, currentDataPageId, dataPage, currentDataPageRid);

//        if (status != true)
//            throw new InvalidSlotNumberException();

        MID updatedRecord = dataPage.updateMap(rid, newtuple, deletedMap);

//        if (updatedRecord != null && !updatedRecord.isReused) {
//            DataPageInfo dpinfo_ondirpage = dirPage.returnDatapageInfo(currentDataPageRid);
//            dpinfo_ondirpage.recct++;
//            dpinfo_ondirpage.flushToTuple();
//        }

//        unpinPage(currentDataPageId, true /* = DIRTY */);
//        unpinPage(currentDirPageId, updatedRecord != null && !updatedRecord.isReused /* undirty ? */);

		unpinPage(rid.pageNo, true /* = DIRTY */);
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
		RID rid;
    	if (insertDatapageRID != null) {
    		rid = insertRecordAt(PhysicalMap.getMapByteArray(tuple));
		} else {
			rid = insertRecord(PhysicalMap.getMapByteArray(tuple));
			currentDirPageToWrite = new Dirpage();
			currentDataPageToWrite = new MapPage();
			pinPage(insertDatapageRID.pageNo, currentDirPageToWrite, false);
			DataPageInfo dpinfo = currentDirPageToWrite.getDatapageInfo(insertDatapageRID);
			pinPage(dpinfo.getPageId(), currentDataPageToWrite, false);
		}

        return new MID(rid.pageNo, rid.slotNo * 3);
    }

    public RID insertRecordAt(byte[] recPtr) throws HFBufMgrException, InvalidSlotNumberException, InvalidTupleSizeException, IOException, HFException, HFDiskMgrException {
		Page pageinbuffer = new Page();

		Dirpage nextDirPage = new Dirpage();
		PageId currentDirPageId = new PageId(insertDatapageRID.pageNo.pid);
		PageId nextDirPageId = new PageId(); // OK
		RID currentDataPageRid = new RID();

		DataPageInfo dpinfo = currentDirPageToWrite.getDatapageInfo(insertDatapageRID);

		if (dpinfo.availspace < recPtr.length) {
			if (currentDirPageToWrite.available_space() < DataPageInfo.size) {
				// new dirpage
				nextDirPageId = newPage(pageinbuffer, 1);
				// need check error!
				if (nextDirPageId == null)
					throw new HFException(null, "can't new pae");

				// initialize new directory page
				nextDirPage.init(nextDirPageId, pageinbuffer);
				PageId temppid = new PageId(INVALID_PAGE);
				nextDirPage.setNextPage(temppid);
				nextDirPage.setPrevPage(currentDirPageId);

				// update current directory page and unpin it
				// currentDirPage is already locked in the Exclusive mode
				currentDirPageToWrite.setNextPage(nextDirPageId);
				unpinPage(currentDirPageId, true/* dirty */);

				currentDirPageId.pid = nextDirPageId.pid;
				currentDirPageToWrite = new Dirpage(nextDirPage);
			}

			{
				PageId currentPageId = new PageId(dpinfo.getPageId().pid);
				MapPage nextPage = new MapPage(_newDatapage(dpinfo));
				currentDataPageToWrite.setNextPage(dpinfo.getPageId());
				nextPage.setPrevPage(currentPageId);
				unpinPage(currentPageId, true);
				currentDataPageToWrite = nextPage;
			}

			Tuple atuple = dpinfo.convertToTuple();

			byte[] tmpData = atuple.getTupleByteArray();
			currentDataPageRid = currentDirPageToWrite.insertRecord(tmpData);
			// need catch error here!
			if (currentDataPageRid == null)
				throw new HFException(null, "no space to insert rec.");
			insertDatapageRID = new RID(currentDataPageRid.pageNo, currentDataPageRid.slotNo);
		}

		RID rid = currentDataPageToWrite.insertRecord(recPtr);
		dpinfo.recct++;
		dpinfo.availspace = currentDataPageToWrite.available_space();
		// DataPage is now released
		DataPageInfo dpinfo_ondirpage = currentDirPageToWrite.returnDatapageInfo(insertDatapageRID);
		dpinfo_ondirpage.availspace = dpinfo.availspace;
		dpinfo_ondirpage.recct = dpinfo.recct;
		dpinfo_ondirpage.getPageId().pid = dpinfo.getPageId().pid;
		dpinfo_ondirpage.flushToTuple();

		return rid;
	}

	public void closeFastInsert() throws InvalidSlotNumberException, InvalidTupleSizeException, IOException, HFBufMgrException {
		PageId currentDirPageId = new PageId(insertDatapageRID.pageNo.pid);
		DataPageInfo dpinfo = currentDirPageToWrite.getDatapageInfo(insertDatapageRID);
    	insertDatapageRID = null;
    	unpinPage(dpinfo.getPageId(), true);
    	unpinPage(currentDirPageId, true);
	}

	public RID insertRecord(byte[] recPtr) throws InvalidSlotNumberException, InvalidTupleSizeException,
			SpaceNotAvailableException, HFException, HFBufMgrException, HFDiskMgrException, IOException {
		int dpinfoLen = 0;
		int recLen = recPtr.length;
		boolean found;
		RID currentDataPageRid = new RID();
		Page pageinbuffer = new Page();
		Dirpage currentDirPage = new Dirpage();
		HFPage currentDataPage = getNewDataPage();

		Dirpage nextDirPage = new Dirpage();
		PageId currentDirPageId = new PageId(_firstDirPageId.pid);
		PageId nextDirPageId = new PageId(); // OK

		pinPage(currentDirPageId, currentDirPage, false/* Rdisk */);

		found = false;
		Tuple atuple;
		DataPageInfo dpinfo = new DataPageInfo();
		while (found == false) { // Start While01
			// look for suitable dpinfo-struct
			for (currentDataPageRid = currentDirPage
					.firstRecord(); currentDataPageRid != null; currentDataPageRid = currentDirPage
					.nextRecord(currentDataPageRid)) {
				dpinfo = currentDirPage.getDatapageInfo(currentDataPageRid);

				// need check the record length == DataPageInfo'slength

				if (recLen <= dpinfo.availspace) {
					found = true;
					break;
				}
			}

			// two cases:
			// (1) found == true:
			// currentDirPage has a datapagerecord which can accomodate
			// the record which we have to insert
			// (2) found == false:
			// there is no datapagerecord on the current directory page
			// whose corresponding datapage has enough space free
			// several subcases: see below
			if (found == false) { // Start IF01
				// case (2)

				// System.out.println("no datapagerecord on the current directory is OK");
				// System.out.println("dirpage availspace "+currentDirPage.available_space());

				// on the current directory page is no datapagerecord which has
				// enough free space
				//
				// two cases:
				//
				// - (2.1) (currentDirPage->available_space() >= sizeof(DataPageInfo):
				// if there is enough space on the current directory page
				// to accomodate a new datapagerecord (type DataPageInfo),
				// then insert a new DataPageInfo on the current directory
				// page
				// - (2.2) (currentDirPage->available_space() <= sizeof(DataPageInfo):
				// look at the next directory page, if necessary, create it.

				if (currentDirPage.available_space() >= DataPageInfo.size) {
					// Start IF02
					// case (2.1) : add a new data page record into the
					// current directory page
					HFPage prevPage = getNewDataPage();
					PageId prevPageId = new PageId(dpinfo.getPageId().pid);

					if (prevPageId.pid != INVALID_PAGE) {
						pinPage(prevPageId, prevPage, false);
					}

					currentDataPage = _newDatapage(dpinfo);

					if (prevPageId.pid != INVALID_PAGE) {
						currentDataPage.setNextPage(prevPage.getNextPage());
						prevPage.setNextPage(dpinfo.getPageId());
						currentDataPage.setPrevPage(prevPageId);
						unpinPage(prevPageId, true);
					}
					// currentDataPage is pinned! and dpinfo->pageId is also locked
					// in the exclusive mode

					// didn't check if currentDataPage==NULL, auto exception

					// currentDataPage is pinned: insert its record
					// calling a HFPage function

					atuple = dpinfo.convertToTuple();

					byte[] tmpData = atuple.getTupleByteArray();
					currentDataPageRid = currentDirPage.insertRecord(tmpData);

					RID tmprid = currentDirPage.firstRecord();

					// need catch error here!
					if (currentDataPageRid == null)
						throw new HFException(null, "no space to insert rec.");

					// end the loop, because a new datapage with its record
					// in the current directorypage was created and inserted into
					// the heapfile; the new datapage has enough space for the
					// record which the user wants to insert

					found = true;

				} // end of IF02
				else { // Start else 02
					// case (2.2)
					nextDirPageId = currentDirPage.getNextPage();
					// two sub-cases:
					//
					// (2.2.1) nextDirPageId != INVALID_PAGE:
					// get the next directory page from the buffer manager
					// and do another look
					// (2.2.2) nextDirPageId == INVALID_PAGE:
					// append a new directory page at the end of the current
					// page and then do another loop

					if (nextDirPageId.pid != INVALID_PAGE) { // Start IF03
						// case (2.2.1): there is another directory page:
						unpinPage(currentDirPageId, false);

						currentDirPageId.pid = nextDirPageId.pid;

						pinPage(currentDirPageId, currentDirPage, false);

						// now go back to the beginning of the outer while-loop and
						// search on the current directory page for a suitable datapage
					} // End of IF03
					else { // Start Else03
						// case (2.2): append a new directory page after currentDirPage
						// since it is the last directory page
						nextDirPageId = newPage(pageinbuffer, 1);
						// need check error!
						if (nextDirPageId == null)
							throw new HFException(null, "can't new pae");

						// initialize new directory page
						nextDirPage.init(nextDirPageId, pageinbuffer);
						PageId temppid = new PageId(INVALID_PAGE);
						nextDirPage.setNextPage(temppid);
						nextDirPage.setPrevPage(currentDirPageId);

						// update current directory page and unpin it
						// currentDirPage is already locked in the Exclusive mode
						currentDirPage.setNextPage(nextDirPageId);
						unpinPage(currentDirPageId, true/* dirty */);

						currentDirPageId.pid = nextDirPageId.pid;
						currentDirPage = new Dirpage(nextDirPage);

						// remark that MINIBASE_BM->newPage already
						// pinned the new directory page!
						// Now back to the beginning of the while-loop, using the
						// newly created directory page.

					} // End of else03
				} // End of else02
				// ASSERTIONS:
				// - if found == true: search will end and see assertions below
				// - if found == false: currentDirPage, currentDirPageId
				// valid and pinned

			} // end IF01
			else { // Start else01
				// found == true:
				// we have found a datapage with enough space,
				// but we have not yet pinned the datapage:

				// ASSERTIONS:
				// - dpinfo valid

				// System.out.println("find the dirpagerecord on current page");

				pinPage(dpinfo.getPageId(), currentDataPage, false);
				// currentDataPage.openHFpage(pageinbuffer);

			} // End else01
		} // end of While01

		// ASSERTIONS:
		// - currentDirPageId, currentDirPage valid and pinned
		// - dpinfo.pageId, currentDataPageRid valid
		// - currentDataPage is pinned!

		if ((dpinfo.getPageId()).pid == INVALID_PAGE) // check error!
			throw new HFException(null, "invalid PageId");

		if (!(currentDataPage.available_space() >= recLen))
			throw new SpaceNotAvailableException(null, "no available space");

		if (currentDataPage == null)
			throw new HFException(null, "can't find Data page");

		RID rid;
		rid = currentDataPage.insertRecord(recPtr);

		dpinfo.recct++;
		dpinfo.availspace = currentDataPage.available_space();

		unpinPage(dpinfo.getPageId(), true /* = DIRTY */);

		// DataPage is now released
		DataPageInfo dpinfo_ondirpage = currentDirPage.returnDatapageInfo(currentDataPageRid);

		dpinfo_ondirpage.availspace = dpinfo.availspace;
		dpinfo_ondirpage.recct = dpinfo.recct;
		dpinfo_ondirpage.getPageId().pid = dpinfo.getPageId().pid;
		dpinfo_ondirpage.flushToTuple();

		unpinPage(currentDirPageId, true /* = DIRTY */);
		insertDatapageRID = new RID(currentDataPageRid.pageNo, currentDataPageRid.slotNo);

		return rid;

	}

    public Stream openStream() throws InvalidMapSizeException, InvalidTupleSizeException, HFBufMgrException,
            InvalidSlotNumberException, IOException {
        return new Stream(this);
    }

	public MID upsert(Map map, Map ejected) throws InvalidTupleSizeException, InvalidSlotNumberException, IOException,
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
			// System.out.println("Looking at DIRECTORY: " + currentDirPageId.pid + ", has space: " + currentDirPage.available_space());
			if (possibleInsertDir == null && currentDirPage.available_space() >= DataPageInfo.size) {
				// If we dont find a datapage to insert into and we have
				// not seen the row col combination, we need to create a new
				// datapage in here
				possibleInsertDir = new PageId(currentDirPageId.pid);
				// System.out.println("POSSIBLE dir insert location: " + currentDirPageId.pid + ", has space: " + currentDirPage.available_space());
			}
			for (currentDataPageRid = currentDirPage
					.firstRecord(); currentDataPageRid != null; currentDataPageRid = currentDirPage
					.nextRecord(currentDataPageRid)) {
				DataPageInfo dpinfo = null;
				try {
					dpinfo = currentDirPage.returnDatapageInfo(currentDataPageRid);
					// System.out.println("Looking at DATAPAGE: " + dpinfo.pageId + ", has space: " + dpinfo.availspace);

					if (possibleInsertLoc == null && dpinfo.availspace >= PhysicalMap.map_size + HFPage.SIZE_OF_SLOT) {
						// If we dont find the row col combination, we need to insert
						// here
						possibleInsertLoc = new PageId(dpinfo.getPageId().pid);
						possibleInsertDir = new PageId(currentDirPageId.pid);
						possibleInsertLocRid = new RID(currentDataPageRid.pageNo, currentDataPageRid.slotNo);
						// System.out.println("POSSIBLE datapage insert location: " + dpinfo.pageId);
					}
				} catch (InvalidSlotNumberException e) {
					return null;
				}

				try {
					pinPage(dpinfo.getPageId(), currentDataPage, false/* Rddisk */);

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
					PhysicalMap pm = currentDataPage.returnPhysicalMap(rid);
					// pm.print();
					if (pm.getRowLabel().equals(map.getRowLabel()) && pm.getColumnLabel().equals(map.getColumnLabel())) {
						// System.out.println("UPDATING");
						int verCnt = pm.getVersionCount();
						int ver = pm.updateMap(map.getTimeStamp(), map.getValue(), ejected);

						if (ver == -1)
							return null;

						if (verCnt < 3) {
							dpinfo.recct++;
							dpinfo.availspace = currentDataPage.available_space();
							dpinfo.flushToTuple();
						}

						unpinPage(currentDirPageId, verCnt < 3);
						unpinPage(dpinfo.getPageId(), true);

						return new MID(rid.pageNo, rid.slotNo * 3 + ver);
					}
					rid = currentDataPage.nextRecord(rid);
				}


				// Dont need this anymore
				unpinPage(dpinfo.getPageId(), false/* Rddisk */);
			}

			// if we would have found the correct datapage on the current
			// directory page we would have already returned.
			// therefore:
			// read in next directory page:

			nextDirPageId = currentDirPage.getNextPage();
			if (nextDirPageId.pid == INVALID_PAGE) {
				if (possibleInsertLoc != null) {
					// System.out.println("INSERTING BACK INTO DATAPAGE");

					if (currentDirPageId.pid != possibleInsertDir.pid)
						pinPage(possibleInsertDir, currentDirPage, false);
					DataPageInfo dpinfo = currentDirPage.returnDatapageInfo(possibleInsertLocRid);
					pinPage(possibleInsertLoc, currentDataPage, false);
					// Datapage pinned. Next insert and flush dirpage and datapage

					RID rid = currentDataPage.insertRecord(PhysicalMap.getMapByteArray(map));
					// if (rid == null)
					// 	System.out.println("OOOOOOOO");

					unpinPage(possibleInsertLoc, true/* dirty */);

					dpinfo.recct++;
					dpinfo.availspace = currentDataPage.available_space();
					dpinfo.flushToTuple();
					unpinPage(possibleInsertDir, true/* dirty */);

					return new MID(rid.pageNo, rid.slotNo * 3);

				} else if (possibleInsertDir != null) {
					// System.out.println("INSERTING BACK INTO DIRPAGE");
					if (currentDirPageId.pid != possibleInsertDir.pid)
						pinPage(possibleInsertDir, currentDirPage, false);
					// Add a new datapage to the possibleInsertDir
					DataPageInfo dpinfo = new DataPageInfo();
					currentDataPageRid = new RID();
					currentDataPage = new MapPage(addNewPageToDir(currentDirPage, dpinfo, currentDataPageRid));

					RID rid = currentDataPage.insertRecord(PhysicalMap.getMapByteArray(map));
					// if (rid == null)
					// 	System.out.println("OOOOOOOO");

					unpinPage(currentDataPage.getCurPage(), true/* dirty */);

					dpinfo.recct++;
					dpinfo.availspace = currentDataPage.available_space();
					dpinfo.flushToTuple();
					unpinPage(currentDirPage.getCurPage(), true/* dirty */);

					return new MID(rid.pageNo, rid.slotNo * 3);
					// break;
				}

				Page pageinbuffer = new Page();
				nextDirPageId = newPage(pageinbuffer, 1);
				// need check error!
				if (nextDirPageId == null)
					throw new HFException(null, "can't new pae");

				Dirpage nextDirPage = new Dirpage();
				// initialize new directory page
				nextDirPage.init(nextDirPageId, pageinbuffer);
				// System.out.println("REACHED THE END! CREATING A NEW DIRPAGE: " + nextDirPageId.pid);
				PageId temppid = new PageId(INVALID_PAGE);
				nextDirPage.setNextPage(temppid);
				nextDirPage.setPrevPage(currentDirPageId);

				// update current directory page and unpin it
				// currentDirPage is already locked in the Exclusive mode
				currentDirPage.setNextPage(nextDirPageId);
				unpinPage(currentDirPageId, true/* dirty */);

				currentDirPage.init(nextDirPageId, nextDirPage);
				currentDirPageId.pid = nextDirPageId.pid;
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

		// System.out.println("We are in the endgame now");
		DataPageInfo dpinfo = null;
		if (possibleInsertLoc != null) {
			// System.out.println("SUPPOSED TO BE INSERTING BACK INTO DATAPAGE");

			// pinPage(possibleInsertDir, currentDirPage, false);
			// dpinfo = currentDirPage.returnDatapageInfo(possibleInsertLocRid);
			// pinPage(possibleInsertLoc, currentDataPage, false);
			// // Datapage pinned. Next insert and flush dirpage and datapage

			// RID rid = currentDataPage.insertRecord(PhysicalMap.getMapByteArray(map));
			// if (rid == null)
			// 	System.out.println("OOOOOOOO");

			// unpinPage(possibleInsertLoc, true/* dirty */);

			// dpinfo.recct++;
			// dpinfo.availspace = currentDataPage.available_space();
			// dpinfo.flushToTuple();
			// unpinPage(possibleInsertDir, true/* dirty */);

			// return new MID(rid.pageNo, rid.slotNo * 3);




		} else if (possibleInsertDir != null) {
			// System.out.println("SUPPOSED TO BE INSERTING BACK INTO DIRPAGE");
			// pinPage(possibleInsertDir, currentDirPage, false);
			// // Add a new datapage to the possibleInsertDir
			// dpinfo = new DataPageInfo();
			// currentDataPageRid = new RID();
			// currentDataPage = new MapPage(addNewPageToDir(currentDirPage, dpinfo, currentDataPageRid));
			// Datapage and dirpage pinned. Next insert and flush dirpage and datapage
		} else {
			// System.out.println("INSERTING AT THE VERY END");
			// Add a new datapage to the currentdirpage
			dpinfo = new DataPageInfo();
			currentDataPageRid = new RID();
			currentDataPage = new MapPage(addNewPageToDir(currentDirPage, dpinfo, currentDataPageRid));
			// Dirpage is already created in the while loop before breaking
			// Datapage and dirpage pinned. Next insert and flush dirpage and datapage
		}

		RID rid = currentDataPage.insertRecord(PhysicalMap.getMapByteArray(map));
		// if (rid == null)
		// 	System.out.println("OOOOOOOO");

		unpinPage(currentDataPage.getCurPage(), true/* dirty */);

		dpinfo.recct++;
		dpinfo.availspace = currentDataPage.available_space();
		dpinfo.flushToTuple();
		unpinPage(currentDirPage.getCurPage(), true/* dirty */);

		return new MID(rid.pageNo, rid.slotNo * 3);
	}
}