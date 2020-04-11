package storage;

import BigT.*;
import diskmgr.Page;
import global.MID;
import global.PageId;
import global.RID;
import heap.*;

import java.io.IOException;

public class SmallMapFile extends Heapfile {
    String ignoredLabel;
    Integer ignoredPos;
    Integer secondaryKey;

    public Integer getSecondaryKey() {
        return this.secondaryKey;
    }

    public SmallMapFile(String name, String ignoredLabel, Integer ignoredPos, Integer secondaryKey) throws HFException, HFBufMgrException, HFDiskMgrException, IOException {
        super(name);
        this.ignoredLabel = ignoredLabel;
        this.ignoredPos = ignoredPos;
        this.secondaryKey = secondaryKey;
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
            HFException, HFBufMgrException, HFDiskMgrException, IOException {
        PageId dirPageId = new PageId(_firstDirPageId.pid);
        SmallDirpage dirpage = new SmallDirpage();
        pinPage(dirPageId, dirpage, false);

        PageId curDataPageId = new PageId();
        SmallMapPage curDataPage = getNewDataPage();

        SmallDataPageInfo dpinfo;

        RID currentDataPageRid = dirpage.firstRecord();
        if (currentDataPageRid == null) {
            curDataPage = makeFirstDataPage(dirpage);
            curDataPageId.pid = curDataPage.getCurPage().pid;
            currentDataPageRid = dirpage.firstRecord();
            dpinfo = dirpage.returnDatapageInfo(currentDataPageRid, this.ignoredLabel, this.ignoredLabel.length() * 2);
        } else {
            dpinfo = dirpage.returnDatapageInfo(currentDataPageRid, this.ignoredLabel, this.ignoredLabel.length() * 2);
            pinPage(dpinfo.getPageId(), curDataPage, false);
            curDataPageId.pid = dpinfo.getPageId().pid;
        }

        // This will unpin curDataPage
        RID rid = insertMap(new SmallMap(tuple, this.ignoredPos), curDataPage);

        dpinfo.recct++;
        dpinfo.flushToTuple();
        unpinPage(dirPageId, true);
        return new MID(rid.pageNo, rid.slotNo);
    }

    public Stream openStream() throws InvalidTupleSizeException, HFBufMgrException, InvalidSlotNumberException, IOException {
        return new Stream(this);
    }

    public Stream openSortedStream() throws IOException, HFBufMgrException, InvalidSlotNumberException, InvalidTupleSizeException {
        return new Stream(this, true);
    }

    /*
    * dirpage must be pinned.
    * Returns a new SmallMapPage which is already pinned and must be unpinned by the caller.
    * */
    private SmallMapPage makeFirstDataPage(Dirpage dirpage) throws IOException, HFException, HFBufMgrException {
        PageId newPageId = new PageId();
        Page apage = _newDatapage(newPageId);
        SmallMapPage newPage = new SmallMapPage(this.ignoredLabel, this.ignoredPos);

        newPage.init(newPageId, apage, this.ignoredLabel, this.ignoredPos);

        newPage.setCurPage(newPageId);
        newPage.setPrevPage(new PageId(-1));
        newPage.setNextPage(new PageId(-1));

        SmallDataPageInfo dpinfo = new SmallDataPageInfo(this.ignoredLabel, this.ignoredLabel.length() * 2);
        dpinfo.getPageId().pid = newPageId.pid;
        dpinfo.recct = 0;

        Tuple atuple = dpinfo.convertToTuple();

        byte[] tmpData = atuple.getTupleByteArray();
        RID currentDataPageRid = dirpage.insertRecord(tmpData);

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
    private SmallMapPage makeNextDataPage(SmallMapPage currentPage, PageId currentPageId) throws HFBufMgrException, HFException, IOException {
        PageId newPageId = new PageId();
        Page apage = _newDatapage(newPageId);
        SmallMapPage newPage = new SmallMapPage(this.ignoredLabel, this.ignoredPos);

        newPage.init(newPageId, apage, this.ignoredLabel, this.ignoredPos);

        currentPage.setNextPage(newPageId);
        newPage.setPrevPage(currentPageId);
        newPage.setCurPage(newPageId);
        newPage.setNextPage(new PageId(-1));

        return newPage;
    }

    private SmallMapPage makeDataPageInBetween(SmallMapPage currentPage, PageId currentPageId, SmallMapPage nextPage, PageId nextPageId) throws HFBufMgrException, HFException, IOException {
        PageId newPageId = new PageId();
        Page apage = _newDatapage(newPageId);
        SmallMapPage newPage = new SmallMapPage(this.ignoredLabel, this.ignoredPos);

        newPage.init(newPageId, apage, this.ignoredLabel, this.ignoredPos);

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

    private RID insertMap(SmallMap map, SmallMapPage startingDatapage) throws HFBufMgrException, InvalidSlotNumberException, InvalidTupleSizeException, IOException, HFException, HFDiskMgrException {
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
                SmallMapPage split = makeDataPageInBetween(curDataPage, curDataPageId, nextDataPage, nextDataPageId);
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
                    SmallMapPage split = makeDataPageInBetween(curDataPage, curDataPageId, nextDataPage, nextDataPageId);
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
                nextDataPage = makeNextDataPage(curDataPage, curDataPageId);
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
                nextDataPage = makeNextDataPage(curDataPage, curDataPageId);
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
                SmallDataPageInfo dpinfo = currentDirPage.getDatapageInfo(rid, this.ignoredLabel, this.ignoredLabel.length() * 2);

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
            SmallDataPageInfo dpinfo = currentDirPage.getDatapageInfo(currentDataPageRid, this.ignoredLabel, this.ignoredLabel.length() * 2);
            PageId currentDatapageId = dpinfo.getPageId();

            currentDataPage = getNewDataPage();
            pinPage(currentDatapageId, currentDataPage, false);
            unpinPage(currentDirPageId, false);
        }

        return currentDataPage;
    }
}
