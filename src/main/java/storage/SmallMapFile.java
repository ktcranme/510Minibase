package storage;

import BigT.*;
import bufmgr.*;
import diskmgr.Page;
import global.*;
import heap.*;
import iterator.Iterator;
import iterator.Sort;
import iterator.SortException;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

public class SmallMapFile extends Heapfile {
    Integer primaryKey;
    Integer pkLength;
    Integer secondaryKey;

    Boolean sortedPrimary;

    public Integer getSecondaryKey() {
        return this.secondaryKey;
    }

    public DataPageIterator getDataPageIterator() throws IOException, PagePinnedException, PageUnpinnedException, HashOperationException, ReplacerException, BufferPoolExceededException, BufMgrException, InvalidSlotNumberException, PageNotReadException, InvalidFrameNumberException, InvalidTupleSizeException, HashEntryNotFoundException {
        return new DataPageIterator(_firstDirPageId, pkLength);
    }

    public SmallMapFile(String name, Integer primaryKey, Integer secondaryKey, Integer pkLength) throws HFException, HFBufMgrException, HFDiskMgrException, IOException {
        super(name);
        this.primaryKey = primaryKey;
        this.secondaryKey = secondaryKey;
        this.pkLength = pkLength;
        sortedPrimary = false;
    }

    private SmallDirpage nextDirPage(SmallDirpage currentDirPage) throws HFBufMgrException, HFException, IOException {
        Page pageinbuffer = new Page();
        PageId nextDirPageId = newPage(pageinbuffer, 1);
        SmallDirpage nextDirPage = new SmallDirpage();
        // need check error!
        if (nextDirPageId == null)
            throw new HFException(null, "can't new pae");

        // initialize new directory page
        nextDirPage.init(nextDirPageId, pageinbuffer);
        PageId temppid = new PageId(INVALID_PAGE);
        nextDirPage.setNextPage(temppid);

        if (currentDirPage != null) {
            temppid.pid = currentDirPage.getCurPage().pid;
            currentDirPage.setNextPage(nextDirPageId);
        }
        nextDirPage.setPrevPage(temppid);

        return nextDirPage;
    }

    public void sortPrimary() throws Exception {
        if (sortedPrimary) return;

        List<SmallDirpage> dirpageList = new ArrayList<>();
        AttrType[] attrType = {
                new AttrType(AttrType.attrInteger),
                new AttrType(AttrType.attrInteger),
                new AttrType(AttrType.attrString)
        };
        short[] attrSize = { pkLength.shortValue() };
        TupleOrder[] order = {
                new TupleOrder(TupleOrder.Ascending),
                new TupleOrder(TupleOrder.Descending)
        };

        Iterator itr = getDirRecItr();
        iterator.Sort sort = new Sort(attrType, (short) 3, attrSize, itr, 3, order[0], pkLength, 10);

        SmallDirpage dirpage = nextDirPage(null);
        dirpageList.add(dirpage);
        Tuple primaryInfo = sort.get_next();
        SmallDataPageInfo info = new SmallDataPageInfo("", pkLength);

        while (primaryInfo != null) {
            if (dirpage.available_space() < HFPage.SIZE_OF_SLOT + info.size) {
                dirpage = nextDirPage(dirpage);
                if (dirpageList.size() == 1)
                    dirpage.setPrevPage(new PageId(_firstDirPageId.pid));
                if (dirpageList.size() > 1)
                    unpinPage(dirpage.getPrevPage(), true);
                dirpageList.add(dirpage);
            }

            byte[] data = new byte[info.size];
            System.arraycopy(primaryInfo.returnTupleByteArray(), 2 + (primaryInfo.noOfFlds() + 1) * 2, data, 0, info.size);
            dirpage.insertRecord(data);

            primaryInfo = sort.get_next();
        }
        sort.close();
        itr.close();
        if (dirpageList.size() > 1)
            unpinPage(dirpageList.get(dirpageList.size() - 1).getCurPage(), true);

        dirpage = new SmallDirpage();
        pinPage(_firstDirPageId, dirpage, false);
        PageId curPageId = new PageId(dirpage.getNextPage().pid);
        PageId nextPageId;
        dirpage.replaceData(dirpageList.get(0));
        dirpage.setNextPage(dirpageList.get(0).getNextPage());
        unpinPage(_firstDirPageId, true);
        unpinPage(dirpageList.get(0).getCurPage(), false);
        freePage(dirpageList.get(0).getCurPage());

        while (curPageId.pid != INVALID_PAGE) {
            pinPage(curPageId, dirpage, false);
            nextPageId = new PageId(dirpage.getNextPage().pid);
            unpinPage(curPageId, false);
            freePage(curPageId);
            curPageId.pid = nextPageId.pid;
        }

        sortedPrimary = true;
    }

    public SmallDirPageInfoIterator getDirRecItr() throws IOException, PagePinnedException, HashOperationException, PageUnpinnedException, InvalidFrameNumberException, BufferPoolExceededException, BufMgrException, PageNotReadException, ReplacerException {
        return new SmallDirPageInfoIterator(_firstDirPageId, pkLength);
    }

    public SmallMapPage getNewDataPage() {
        return new SmallMapPage(this.pkLength);
    }

    public SmallMapPage getNewDataPage(Page page) {
        return new SmallMapPage(page, this.pkLength);
    }

    public SmallMapPage getNewDataPage(Page page, PageId pid, String primary) throws IOException {
        SmallMapPage hfp = new SmallMapPage(this.pkLength);
        hfp.init(pid, page, this.pkLength, primary);
        return hfp;
    }

    public int getMapCnt() throws InvalidSlotNumberException, InvalidTupleSizeException, HFDiskMgrException,
            HFBufMgrException, IOException {
        return getRecCnt();
    }

    public Map getMap(MID rid) throws InvalidSlotNumberException, InvalidTupleSizeException, HFException,
            HFDiskMgrException, HFBufMgrException, Exception {

        SmallMapPage dataPage = getNewDataPage();
        pinPage(rid.pageNo, dataPage, false);
        byte[] rec = dataPage.getRecord(new RID(rid.pageNo, rid.slotNo));

        SmallMap map = new SmallMap(rec, 0);
        Map bigMap = map.toMap(dataPage.getPrimaryKey(), this.primaryKey);
        unpinPage(rid.pageNo, false);
        return bigMap;
    }

//    public MID updateMap(MID rid, Map newtuple) throws InvalidSlotNumberException, InvalidUpdateException,
//            InvalidTupleSizeException, HFException, HFDiskMgrException, HFBufMgrException, Exception {
//
//        SmallMapPage dataPage = getNewDataPage();
//        pinPage(rid.pageNo, dataPage, false);
//        dataPage.updateRecord(new RID(rid.pageNo, rid.slotNo), new SmallMap(newtuple, this.primaryKey).getMapByteArray());
//        unpinPage(rid.pageNo, true);
//
//        return rid;
//    }

    private SmallDirpage findPrimaryLocationInDirectory(PageId primaryPageId, RID datapageRid) throws HFBufMgrException, IOException, InvalidTupleSizeException, InvalidSlotNumberException {
        PageId dirPageId = new PageId(_firstDirPageId.pid);
        SmallDirpage dirpage = new SmallDirpage();
        SmallDataPageInfo dpinfo;

        while (dirPageId.pid != INVALID_PAGE) {
            pinPage(dirPageId, dirpage, false);

            for (RID currentDataPageRid = dirpage.firstRecord();
                 currentDataPageRid != null;
                 currentDataPageRid = dirpage.nextRecord(currentDataPageRid)) {

                dpinfo = dirpage.returnDatapageInfo(currentDataPageRid, this.pkLength);
                if (dpinfo.pageId.pid == primaryPageId.pid) {
                    datapageRid.pageNo.pid = currentDataPageRid.pageNo.pid;
                    datapageRid.slotNo = currentDataPageRid.slotNo;
                    return dirpage;
                }
            }

            PageId nextPage = new PageId(dirpage.getNextPage().pid);
            unpinPage(dirPageId, false);
            dirPageId.pid = nextPage.pid;
        }

        datapageRid.pageNo.pid = -1;
        datapageRid.slotNo = -1;
        return null;
    }

    private SmallDirpage findPrimaryLocationInDirectory(String primary, RID datapageRid) throws HFBufMgrException, IOException, InvalidTupleSizeException, InvalidSlotNumberException {
        PageId dirPageId = new PageId(_firstDirPageId.pid);
        SmallDirpage dirpage = new SmallDirpage();
        SmallDataPageInfo dpinfo;

        while (dirPageId.pid != INVALID_PAGE) {
            pinPage(dirPageId, dirpage, false);

            for (RID currentDataPageRid = dirpage.firstRecord();
                 currentDataPageRid != null;
                 currentDataPageRid = dirpage.nextRecord(currentDataPageRid)) {

                dpinfo = dirpage.returnDatapageInfo(currentDataPageRid, this.pkLength);
                if (dpinfo.primaryKey.equals(primary)) {
                    datapageRid.pageNo.pid = currentDataPageRid.pageNo.pid;
                    datapageRid.slotNo = currentDataPageRid.slotNo;
                    return dirpage;
                }
            }

            PageId nextPage = new PageId(dirpage.getNextPage().pid);
            unpinPage(dirPageId, false);
            dirPageId.pid = nextPage.pid;
        }

        datapageRid.pageNo.pid = -1;
        datapageRid.slotNo = -1;
        return null;
    }

    public boolean deleteMap(MID rid) throws InvalidSlotNumberException, InvalidTupleSizeException, HFException,
            HFBufMgrException, HFDiskMgrException, Exception {

        SmallMapPage dataPage = getNewDataPage();
        try {
            pinPage(rid.pageNo, dataPage, false);
        } catch (Exception e) {
            return false;
        }

        RID recRid = new RID(rid.pageNo, rid.slotNo);
        RID datapageRid = new RID();
        SmallDirpage dirpage = findPrimaryLocationInDirectory(dataPage.getPrimaryKey(), datapageRid);

        if (dirpage == null)
            throw new HFException(null, "This shouldnt happen!");

        dataPage.deleteRecord(recRid);
        // Still need to update dpinfo and unpin this datapage and dirpage

        SmallDataPageInfo dpinfo = dirpage.returnDatapageInfo(datapageRid, this.pkLength);
        dpinfo.recct--;
        dpinfo.flushToTuple();

        if (dataPage.isEmpty()) {
            PageId prevPageId = new PageId(dataPage.getPrevPage().pid);
            PageId nextPageId = new PageId(dataPage.getNextPage().pid);

            if (prevPageId.pid != INVALID_PAGE) {

                SmallMapPage prevPage = getNewDataPage();
                pinPage(prevPageId, prevPage, false);
                SmallMapPage nextPage;
                if (nextPageId.pid != INVALID_PAGE) {
                    nextPage = getNewDataPage();
                    pinPage(nextPageId, nextPage, false);
                    nextPage.setPrevPage(prevPageId);
                    unpinPage(nextPageId, true);
                }
                prevPage.setNextPage(nextPageId);
                unpinPage(prevPageId, true);

                // Unpin and delete dataPage
                unpinPage(rid.pageNo, false);
                freePage(rid.pageNo);

                // Still have to unpin dirpage

            } else if (nextPageId.pid != INVALID_PAGE) {
                dpinfo.pageId.pid = nextPageId.pid;
                dpinfo.flushToTuple();
                unpinPage(rid.pageNo, true);
                freePage(rid.pageNo);

                SmallMapPage nextPage = getNewDataPage();
                pinPage(nextPageId, nextPage, false);
                nextPage.setPrevPage(prevPageId);
                unpinPage(nextPageId, true);

                // Still have to unpin dirpage
            } else {

                unpinPage(rid.pageNo, false);
                freePage(rid.pageNo);
                dirpage.deleteRecord(datapageRid);

                if (dirpage.isEmpty() && dirpage.getCurPage().pid != _firstDirPageId.pid) {
                    PageId prevDirPageId = new PageId(dirpage.getPrevPage().pid);
                    PageId nextDirPageId = new PageId(dirpage.getNextPage().pid);

                    SmallDirpage prevDirPage = new SmallDirpage();
                    pinPage(prevDirPageId, prevDirPage, false);
                    SmallDirpage nextDirPage;

                    if (nextDirPageId.pid != INVALID_PAGE) {
                        nextDirPage = new SmallDirpage();
                        pinPage(nextDirPageId, nextDirPage, false);
                        nextDirPage.setPrevPage(prevDirPageId);
                        unpinPage(nextDirPageId, true);
                    }
                    prevDirPage.setNextPage(nextDirPageId);
                    unpinPage(prevDirPageId, true);

                    unpinPage(dirpage.getCurPage(), false);
                    freePage(dirpage.getCurPage());

                    // Got rid of the entire dirpage, so return
                    return true;
                }

                // Still have to unpin dirpage
            }
        } else {
            // dataPage not empty. We modified it, so lets flush it
            unpinPage(rid.pageNo, true);
        }

        unpinPage(dirpage.getCurPage(), true);
        return true;
    }

    private SmallDirpage findPrimaryDataPage(String primary, RID datapageRid, Boolean create) throws HFBufMgrException, IOException, InvalidTupleSizeException, InvalidSlotNumberException, HFException {
        PageId dirPageId = new PageId(_firstDirPageId.pid);
        SmallDirpage dirpage = new SmallDirpage();
        SmallDataPageInfo dpinfo = new SmallDataPageInfo("", this.pkLength);

        while (dirPageId.pid != INVALID_PAGE) {
            pinPage(dirPageId, dirpage, false);

            for (RID currentDataPageRid = dirpage.firstRecord();
                 currentDataPageRid != null;
                 currentDataPageRid = dirpage.nextRecord(currentDataPageRid)) {

                dpinfo = dirpage.getDatapageInfo(currentDataPageRid, this.pkLength);
                if (dpinfo.primaryKey.equals(primary)) {
                    datapageRid.pageNo.pid = currentDataPageRid.pageNo.pid;
                    datapageRid.slotNo = currentDataPageRid.slotNo;
                    return dirpage;
                }

            }

            // Keep current dirpage pinned if next page is invalid
            if (dirpage.getNextPage().pid != INVALID_PAGE)
                unpinPage(dirPageId, false);
            dirPageId = new PageId(dirpage.getNextPage().pid);
        }

        if (!create) {
            unpinPage(dirpage.getCurPage(), false);
            return null;
        }

        // Did not find?
        if (dirpage.available_space() < dpinfo.size + HFPage.SIZE_OF_SLOT) {
            // Add new dirpage
            Page pageinbuffer = new Page();
            PageId nextDirPageId = newPage(pageinbuffer, 1);
            SmallDirpage nextDirPage = new SmallDirpage();
            // need check error!
            if (nextDirPageId == null)
                throw new HFException(null, "can't new pae");

            // initialize new directory page
            nextDirPage.init(nextDirPageId, pageinbuffer);
            PageId temppid = new PageId(INVALID_PAGE);
            nextDirPage.setNextPage(temppid);
            nextDirPage.setPrevPage(dirpage.getCurPage());

            // update current directory page and unpin it
            // currentDirPage is already locked in the Exclusive mode
            dirpage.setNextPage(nextDirPageId);
            unpinPage(dirpage.getCurPage(), true);

            dirPageId.pid = nextDirPageId.pid;
            dirpage = nextDirPage;
        }

        datapageRid.pageNo.pid = INVALID_PAGE;
        datapageRid.slotNo = INVALID_PAGE;
        return dirpage;
    }

    public MID insertMap(Map tuple) throws InvalidSlotNumberException, InvalidTupleSizeException,
            HFException, HFBufMgrException, HFDiskMgrException, IOException {
        String primary = tuple.getKey(this.primaryKey);

        PageId curDataPageId = new PageId();
        SmallMapPage curDataPage = getNewDataPage();

        RID currentDataPageRid = new RID();
        SmallDirpage dirpage = findPrimaryDataPage(primary, currentDataPageRid, true);
        PageId dirPageId = dirpage.getCurPage();

        SmallDataPageInfo dpinfo;
        if (currentDataPageRid.pageNo.pid == INVALID_PAGE) {
            sortedPrimary = false;
            curDataPage = makeNewPrimaryDataPage(dirpage, primary, currentDataPageRid);
            curDataPageId.pid = curDataPage.getCurPage().pid;
            dpinfo = dirpage.returnDatapageInfo(currentDataPageRid, this.pkLength);
        } else {
            dpinfo = dirpage.returnDatapageInfo(currentDataPageRid, this.pkLength);
            pinPage(dpinfo.getPageId(), curDataPage, false);
            curDataPageId.pid = dpinfo.getPageId().pid;
        }

        // This will unpin curDataPage
        RID rid;
        if (this.secondaryKey == null) {
            rid = insertMapUnsorted(new SmallMap(tuple, this.primaryKey), curDataPage, primary);
        } else {
            rid = insertMap(new SmallMap(tuple, this.primaryKey), curDataPage, primary);
        }

        dpinfo.recct++;
        dpinfo.flushToTuple();
        unpinPage(dirPageId, true);
        return new MID(rid.pageNo, rid.slotNo);
    }

    public Stream openStream() throws InvalidTupleSizeException, HFBufMgrException, InvalidSlotNumberException, IOException, PagePinnedException, PageUnpinnedException, HashOperationException, ReplacerException, BufferPoolExceededException, BufMgrException, PageNotReadException, InvalidFrameNumberException, HashEntryNotFoundException {
        return new Stream(this);
    }

    public Stream openSortedStream() throws Exception {
        sortPrimary();
        return new Stream(this, this.secondaryKey != null);
    }

    /*
     * dirpage must be pinned.
     * Returns a new SmallMapPage which is already pinned and must be unpinned by the caller.
     * */
    private SmallMapPage makeNewPrimaryDataPage(Dirpage dirpage, String primary, RID primaryPageRID) throws IOException, HFBufMgrException, HFException {
        PageId newPageId = new PageId();
        Page apage = _newDatapage(newPageId);
        SmallMapPage newPage = new SmallMapPage(this.pkLength);

        newPage.init(newPageId, apage, this.pkLength, primary);

        newPage.setCurPage(newPageId);
        newPage.setPrevPage(new PageId(-1));
        newPage.setNextPage(new PageId(-1));

        SmallDataPageInfo dpinfo = new SmallDataPageInfo(primary, this.pkLength);
        dpinfo.getPageId().pid = newPageId.pid;
        dpinfo.recct = 0;

        Tuple atuple = dpinfo.convertToTuple();

        byte[] tmpData = atuple.getTupleByteArray();
        RID currentDataPageRid = dirpage.insertRecord(tmpData);
        primaryPageRID.pageNo.pid = currentDataPageRid.pageNo.pid;
        primaryPageRID.slotNo = currentDataPageRid.slotNo;

        // need catch error here!
        if (currentDataPageRid == null)
            throw new HFException(null, "no space to insert rec.");

        return newPage;
    }

    /*
    * Dont make an entry in directory page. Directory page is only for the first datapage.
    * currentPage is expected to be pinned.
    * returns a SmallMapPage that is already pinned and must be unpinned by the caller.
    * */
    private SmallMapPage makeNextDataPage(SmallMapPage currentPage, PageId currentPageId, String primary) throws HFBufMgrException, HFException, IOException {
        PageId newPageId = new PageId();
        Page apage = _newDatapage(newPageId);
        SmallMapPage newPage = new SmallMapPage(this.pkLength);

        newPage.init(newPageId, apage, this.pkLength, primary);

        currentPage.setNextPage(newPageId);
        newPage.setPrevPage(currentPageId);
        newPage.setCurPage(newPageId);
        newPage.setNextPage(new PageId(-1));

        return newPage;
    }

    private SmallMapPage makeDataPageInBetween(SmallMapPage currentPage, PageId currentPageId, SmallMapPage nextPage, PageId nextPageId, String primary) throws HFBufMgrException, HFException, IOException {
        PageId newPageId = new PageId();
        Page apage = _newDatapage(newPageId);
        SmallMapPage newPage = new SmallMapPage(this.pkLength);

        newPage.init(newPageId, apage, this.pkLength, primary);

        currentPage.setNextPage(newPageId);
        newPage.setPrevPage(currentPageId);
        newPage.setCurPage(newPageId);
        newPage.setNextPage(nextPageId);
        nextPage.setPrevPage(newPageId);

        return newPage;
    }

    private boolean pageHasSpace(SmallMapPage page) throws IOException {
        return page.available_space() > HFPage.SIZE_OF_SLOT + SmallMap.map_size;
    }

    private RID insertMapUnsorted(SmallMap map, SmallMapPage startingDatapage, String primary) throws HFBufMgrException, InvalidSlotNumberException, InvalidTupleSizeException, IOException, HFException, HFDiskMgrException {
        if (this.secondaryKey != null)
            throw new HFException(null, "Must sort on secondary!");

        PageId curDataPageId = new PageId(startingDatapage.getCurPage().pid);
        SmallMapPage curDataPage = startingDatapage;
        PageId nextDataPageId = new PageId();

        while (curDataPageId.pid != INVALID_PAGE) {
            if (pageHasSpace(curDataPage)) {
                RID rid = curDataPage.insertRecord(map.getMapByteArray());
                unpinPage(curDataPageId, true);
                return rid;
            }

            nextDataPageId = new PageId(curDataPage.getNextPage().pid);
            if (nextDataPageId.pid != INVALID_PAGE) {
                unpinPage(curDataPageId, false);
                pinPage(nextDataPageId, curDataPage, false);
            }
            curDataPageId.pid = nextDataPageId.pid;
        }

        // Update curDataPage with the next page ID and flush it
        PageId pageId = new PageId(curDataPage.getCurPage().pid);
        curDataPage = makeNextDataPage(curDataPage, curDataPageId, primary);
        unpinPage(pageId, true);
        // Insert record into the newly created page
        RID rid = curDataPage.insertRecord(map.getMapByteArray());
        unpinPage(curDataPage.getCurPage(), true);

        return rid;
    }

    private RID insertMap(SmallMap map, SmallMapPage startingDatapage, String primary) throws HFBufMgrException, InvalidSlotNumberException, InvalidTupleSizeException, IOException, HFException, HFDiskMgrException {
        if (this.secondaryKey == null)
            throw new HFException(null, "Must not sort on secondary!");

        PageId curDataPageId = new PageId(startingDatapage.getCurPage().pid);
        SmallMapPage curDataPage = startingDatapage;

        PageId nextDataPageId = new PageId();
        SmallMapPage nextDataPage = getNewDataPage();

        // At this point, only curDataPage is pinned
        nextDataPageId = new PageId(curDataPage.getNextPage().pid);

        while (nextDataPageId.pid != INVALID_PAGE) {
            pinPage(nextDataPageId, nextDataPage, false);

            // We can probably insert into nextDataPage
            if (nextDataPage.getMinVal(this.secondaryKey).compareTo(map.getKey(this.secondaryKey)) < 0) {
                unpinPage(curDataPageId, false);
                curDataPage = nextDataPage;
                curDataPageId.pid = nextDataPageId.pid;
                nextDataPageId = new PageId(curDataPage.getNextPage().pid);
                nextDataPage = getNewDataPage();

                continue;
            }

            // nextDataPage has minVal > map.getValue()
            // So, must be inserted into currentDataPage (with possible split) if maxVal > map.getValue()
            // Or we insert into next page, with a possible split
            if (!pageHasSpace(curDataPage) && !pageHasSpace(nextDataPage)) {
                // split current page
                SmallMapPage split = makeDataPageInBetween(curDataPage, curDataPageId, nextDataPage, nextDataPageId, primary);
                // his work is done here
                unpinPage(nextDataPageId, true);
                // move data from curDatapage to split
                curDataPage.migrateHalf(split, this.secondaryKey);
                // Choose where the new record must go - split or curDataPage
                RID rid;
                if (curDataPage.getMaxVal(this.secondaryKey).compareTo(map.getKey(this.secondaryKey)) > 0) {
                    rid = curDataPage.insertRecord(map.getMapByteArray());
                } else {
                    rid = split.insertRecord(map.getMapByteArray());
                }
                unpinPage(curDataPageId, true);
                try {
                    unpinPage(split.getCurPage(), true);
                } catch (Exception e) {
                    throw new HFBufMgrException();
                }

                return rid;
            } else if (!pageHasSpace(curDataPage)) {

                if (curDataPage.getMaxVal(this.secondaryKey).compareTo(map.getKey(this.secondaryKey)) > 0) {

                    // split current page
                    SmallMapPage split = makeDataPageInBetween(curDataPage, curDataPageId, nextDataPage, nextDataPageId, primary);
                    // his work is done here
                    unpinPage(nextDataPageId, true);
                    // move data from curDatapage to split
                    curDataPage.migrateHalf(split, this.secondaryKey);
                    // Choose where the new record must go - split or curDataPage
                    RID rid;
                    if (curDataPage.getMaxVal(this.secondaryKey).compareTo(map.getKey(this.secondaryKey)) > 0) {
                        rid = curDataPage.insertRecord(map.getMapByteArray());
                    } else {
                        rid = split.insertRecord(map.getMapByteArray());
                    }

                    unpinPage(curDataPageId, true);
                    unpinPage(split.getCurPage(), true);

                    return rid;

                }

                // Insert to nextDataPage. This becomes the new minVal
                unpinPage(curDataPageId, false);
                RID rid = nextDataPage.insertRecord(map.getMapByteArray());
                unpinPage(nextDataPageId, true);
                return rid;
            } else {
                // Insert to curDataPage.
                unpinPage(nextDataPageId, false);
                RID rid = curDataPage.insertRecord(map.getMapByteArray());
                unpinPage(curDataPageId, true);

                return rid;
            }
        }

        // If we are here, it probably means that there's just one datapage
        // We may have to make a new datapage
        if (!pageHasSpace(curDataPage)) {

            if (curDataPage.getMaxVal(this.secondaryKey).compareTo(map.getKey(this.secondaryKey)) > 0) {
                // Split
                nextDataPage = makeNextDataPage(curDataPage, curDataPageId, primary);
                curDataPage.migrateHalf(nextDataPage, this.secondaryKey);
                if (curDataPage.getMaxVal(this.secondaryKey).compareTo(map.getKey(this.secondaryKey)) > 0) {
                    unpinPage(nextDataPage.getCurPage(), true);
                } else {
                    unpinPage(curDataPageId, true);
                    curDataPageId.pid = nextDataPage.getCurPage().pid;
                    curDataPage = nextDataPage;
                    nextDataPage = null;
                }

            } else {
                // create new datapage
                nextDataPage = makeNextDataPage(curDataPage, curDataPageId, primary);
                unpinPage(curDataPageId, true);
                curDataPageId.pid = nextDataPage.getCurPage().pid;
                curDataPage = nextDataPage;
                nextDataPage = null;
            }
        }

        RID rid = curDataPage.insertRecord(map.getMapByteArray());
        unpinPage(curDataPage.getCurPage(), true);
        return rid;
    }


    public int getRecCnt() throws InvalidSlotNumberException, InvalidTupleSizeException, HFDiskMgrException,
            HFBufMgrException, IOException

    {
        int answer = 0;
        PageId currentDirPageId = new PageId(_firstDirPageId.pid);

        PageId nextDirPageId = new PageId(0);

        SmallDirpage currentDirPage = new SmallDirpage();

        while (currentDirPageId.pid != INVALID_PAGE) {
            pinPage(currentDirPageId, currentDirPage, false);

            RID rid = new RID();
            Tuple atuple;
            for (rid = currentDirPage.firstRecord(); rid != null; // rid==NULL means no more record
                 rid = currentDirPage.nextRecord(rid)) {
                SmallDataPageInfo dpinfo = currentDirPage.getDatapageInfo(rid, this.pkLength);

                answer += dpinfo.recct;
            }

            // ASSERTIONS: no more record
            // - we have read all datapage records on
            // the current directory page.

            nextDirPageId = currentDirPage.getNextPage();
            unpinPage(currentDirPageId, false /* undirty */);
            currentDirPageId.pid = nextDirPageId.pid;
        }

        // ASSERTIONS:
        // - if error, exceptions
        // - if end of heapfile reached: currentDirPageId == INVALID_PAGE
        // - if not yet end of heapfile: currentDirPageId valid

        return answer;
    } // end of getRecCnt

    public SmallMapPage getFirstDataPage() throws HFBufMgrException, IOException, InvalidTupleSizeException, InvalidSlotNumberException {
        SmallDirpage currentDirPage = new SmallDirpage();
        PageId currentDirPageId = new PageId(_firstDirPageId.pid);
        pinPage(currentDirPageId, currentDirPage, false/* Rdisk */);

        SmallMapPage currentDataPage;

        RID currentDataPageRid = currentDirPage.firstRecord();
        if (currentDataPageRid == null) {
            return null;
        } else {
            SmallDataPageInfo dpinfo = currentDirPage.getDatapageInfo(currentDataPageRid, this.pkLength);
            PageId currentDatapageId = dpinfo.getPageId();

            currentDataPage = getNewDataPage();
            pinPage(currentDatapageId, currentDataPage, false);
            unpinPage(currentDirPageId, false);
        }

        return currentDataPage;
    }

    public void deleteFile() throws InvalidSlotNumberException, FileAlreadyDeletedException, InvalidTupleSizeException,
            HFBufMgrException, HFDiskMgrException, IOException {
        if (_file_deleted)
            throw new FileAlreadyDeletedException(null, "file alread deleted");

        // Mark the deleted flag (even if it doesn't get all the way done).
        _file_deleted = true;

        // Deallocate all data pages
        PageId currentDirPageId = new PageId();
        currentDirPageId.pid = _firstDirPageId.pid;
        PageId nextDirPageId = new PageId();
        nextDirPageId.pid = 0;
        Page pageinbuffer = new Page();
        SmallDirpage currentDirPage = new SmallDirpage();
        Tuple atuple;

        pinPage(currentDirPageId, currentDirPage, false);
        // currentDirPage.openHFpage(pageinbuffer);

        RID rid = new RID();
        while (currentDirPageId.pid != INVALID_PAGE) {
            for (rid = currentDirPage.firstRecord(); rid != null; rid = currentDirPage.nextRecord(rid)) {
                SmallDataPageInfo dpinfo = currentDirPage.getDatapageInfo(rid, pkLength);
                PageId datapage = new PageId(dpinfo.pageId.pid);
                SmallMapPage page = new SmallMapPage(pkLength);
                pinPage(datapage, page, false);
                datapage.pid = page.getNextPage().pid;
                unpinPage(page.getCurPage(), false);

                while (datapage.pid != INVALID_PAGE) {
                    pinPage(datapage, page, false);
                    PageId purge = new PageId(datapage.pid);
                    datapage.pid = page.getNextPage().pid;
                    unpinPage(purge, false);
                    freePage(purge);
                }
                freePage(dpinfo.pageId);
            }
            // ASSERTIONS:
            // - we have freePage()'d all data pages referenced by
            // the current directory page.

            nextDirPageId = currentDirPage.getNextPage();
            unpinPage(currentDirPageId, false);
            freePage(currentDirPageId);

            currentDirPageId.pid = nextDirPageId.pid;
            if (nextDirPageId.pid != INVALID_PAGE) {
                pinPage(currentDirPageId, currentDirPage, false);
            }
        }

        delete_file_entry(_fileName);
    }
}
