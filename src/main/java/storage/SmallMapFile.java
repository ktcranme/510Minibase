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
//        RID rid = insertRecord(new SmallMap(tuple, this.ignoredPos).getMapByteArray());

        RID rid = insertMap(new SmallMap(tuple, this.ignoredPos));
        return new MID(rid.pageNo, rid.slotNo);
    }

    @Override
    public Stream openStream() throws InvalidMapSizeException, InvalidTupleSizeException, HFBufMgrException, InvalidSlotNumberException, IOException {
        return new Stream(this);
    }

    /*
    * dirpage must be pinned.
    * Returns a new SmallMapPage which is already pinned and must be unpinned by the caller.
    * */
    private SmallMapPage makeFirstDataPage(Dirpage dirpage) throws IOException, HFException, HFBufMgrException {
        PageId newPageId = new PageId();
        Page apage = _newDatapage(newPageId);
        SmallMapPage newPage = new SmallMapPage(this.ignoredLabel, this.ignoredPos);
        DataPageInfo dpinfo = new DataPageInfo();

        newPage.init(newPageId, apage);

        newPage.setCurPage(newPageId);
        newPage.setPrevPage(new PageId(-1));
        newPage.setNextPage(new PageId(-1));


        dpinfo.getPageId().pid = newPageId.pid;
        dpinfo.recct = 0;
        dpinfo.availspace = newPage.available_space();

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

        newPage.init(newPageId, apage);

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

        newPage.init(newPageId, apage);

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

    private RID insertMap(SmallMap map) throws HFBufMgrException, InvalidSlotNumberException, InvalidTupleSizeException, IOException, HFException, HFDiskMgrException {
        PageId dirPageId = new PageId(_firstDirPageId.pid);
        Dirpage dirpage = new Dirpage();
        pinPage(dirPageId, dirpage, false);

        PageId curDataPageId = new PageId();
        SmallMapPage curDataPage = getNewDataPage();

        PageId nextDataPageId = new PageId();
        SmallMapPage nextDataPage = getNewDataPage();

        DataPageInfo dpinfo;

        RID currentDataPageRid = dirpage.firstRecord();
        if (currentDataPageRid == null) {
            curDataPage = makeFirstDataPage(dirpage);
            curDataPageId.pid = curDataPage.getCurPage().pid;
        } else {
            dpinfo = dirpage.getDatapageInfo(currentDataPageRid);
            pinPage(dpinfo.getPageId(), curDataPage, false);
            curDataPageId.pid = dpinfo.getPageId().pid;
        }

        unpinPage(dirPageId, currentDataPageRid == null);

        // At this point, only curDataPage is pinned
        nextDataPageId = new PageId(curDataPage.getNextPage().pid);
        while (nextDataPageId.pid != INVALID_PAGE) {
            pinPage(nextDataPageId, nextDataPage, false);

            // We can probably insert into nextDataPage
            if (nextDataPage.getMinVal().compareTo(map.getValue()) < 0) {
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
                System.out.println("Splitting page " + curDataPage.getCurPage().pid + " to get " + curDataPage.getCurPage().pid + " and " + split.getCurPage().pid);

                // his work is done here
                unpinPage(nextDataPageId, true);
                // move data from curDatapage to split
                // HOW??
                curDataPage.migrateHalf(split);
                System.out.println("NEW RANGE FOR " + curDataPage.getCurPage().pid + ": " + curDataPage.getMinVal() + " to " + curDataPage.getMaxVal());
                System.out.println("NEW RANGE FOR " + split.getCurPage().pid + ": " + split.getMinVal() + " to " + split.getMaxVal());

                System.out.println("SPLIT " + curDataPage.getCurPage().pid + ": Inserting " + map.getValue() + " In range " + curDataPage.getMinVal() + " AND " + curDataPage.getMaxVal());
                // Choose where the new record must go - split or curDataPage
                RID rid = curDataPage.insertRecord(map.getMapByteArray());

                unpinPage(curDataPageId, true);
                try {
                    unpinPage(split.getCurPage(), true);
                } catch (Exception e) {
                    throw new HFBufMgrException();
                }

                return rid;
            } else if (!pageHasSpace(curDataPage)) {

                if (curDataPage.getMaxVal().compareTo(map.getValue()) > 0) {

                    // split current page
                    SmallMapPage split = makeDataPageInBetween(curDataPage, curDataPageId, nextDataPage, nextDataPageId);
                    System.out.println("Splitting page " + curDataPage.getCurPage().pid + " to get " + curDataPage.getCurPage().pid + " and " + split.getCurPage().pid);
                    // his work is done here
                    unpinPage(nextDataPageId, true);
                    // move data from curDatapage to split
                    // HOW??
                    curDataPage.migrateHalf(split);
                    System.out.println("NEW RANGE FOR " + curDataPage.getCurPage().pid + ": " + curDataPage.getMinVal() + " to " + curDataPage.getMaxVal());
                    System.out.println("NEW RANGE FOR " + split.getCurPage().pid + ": " + split.getMinVal() + " to " + split.getMaxVal());

                    // Choose where the new record must go - split or curDataPage
                    RID rid = null;
                    if (curDataPage.getMaxVal().compareTo(map.getValue()) > 0) {
                        System.out.println("AFTER SPLIT " + curDataPage.getCurPage().pid + ": Inserting " + map.getValue() + " In range " + curDataPage.getMinVal() + " AND " + curDataPage.getMaxVal());
                        rid = curDataPage.insertRecord(map.getMapByteArray());
                    } else {
                        System.out.println("AFTER SPLIT " + split.getCurPage().pid + ": Inserting " + map.getValue() + " In range " + split.getMinVal() + " AND " + split.getMaxVal());
                        rid = split.insertRecord(map.getMapByteArray());
                    }

                    unpinPage(curDataPageId, true);
                    unpinPage(split.getCurPage(), true);

                    return rid;


                }

                // Insert to nextDataPage. This becomes the new minVal
                unpinPage(curDataPageId, false);
                System.out.println("CURRPAGE FULL " + nextDataPage.getCurPage().pid + ": Inserting " + map.getValue() + " In range " + nextDataPage.getMinVal() + " AND " + nextDataPage.getMaxVal());

                RID rid = nextDataPage.insertRecord(map.getMapByteArray());
                unpinPage(nextDataPageId, true);
                return rid;
            } else {
                // Insert to curDataPage.
                unpinPage(nextDataPageId, false);
                System.out.println("CURRPAGE NOT FULL " + curDataPage.getCurPage().pid + ": Inserting " + map.getValue() + " In range " + curDataPage.getMinVal() + " AND " + curDataPage.getMaxVal());

                RID rid = curDataPage.insertRecord(map.getMapByteArray());
                unpinPage(curDataPageId, true);

                return rid;
            }
        }

        // If we are here, it probably means that there's just one datapage
        // We may have to make a new datapage
        if (!pageHasSpace(curDataPage)) {

            if (curDataPage.getMaxVal().compareTo(map.getValue()) > 0) {
                // Split
                nextDataPage = makeNextDataPage(curDataPage, curDataPageId);
                System.out.println("Splitting page " + curDataPage.getCurPage().pid + " to get " + curDataPage.getCurPage().pid + " and " + nextDataPage.getCurPage().pid);
                curDataPage.migrateHalf(nextDataPage);
                System.out.println("NEW RANGE FOR " + curDataPage.getCurPage().pid + ": " + curDataPage.getMinVal() + " to " + curDataPage.getMaxVal());
                System.out.println("NEW RANGE FOR " + nextDataPage.getCurPage().pid + ": " + nextDataPage.getMinVal() + " to " + nextDataPage.getMaxVal());

                if (curDataPage.getMaxVal().compareTo(map.getValue()) > 0) {
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

        System.out.println("END OF THE LINE " + curDataPage.getCurPage().pid + ": Inserting " + map.getValue() + " In range " + curDataPage.getMinVal() + " AND " + curDataPage.getMaxVal());

        RID rid = curDataPage.insertRecord(map.getMapByteArray());
        unpinPage(curDataPage.getCurPage(), true);
        return rid;
    }

    public void test() throws HFBufMgrException, IOException, InvalidSlotNumberException, InvalidTupleSizeException {
        RID currentDataPageRid = new RID();
        Dirpage currentDirPage = new Dirpage();
        SmallMapPage currentDataPage = getNewDataPage();

        PageId currentDirPageId = new PageId(_firstDirPageId.pid);
        PageId nextDirPageId = new PageId(); // OK

        pinPage(currentDirPageId, currentDirPage, false/* Rdisk */);

        Tuple atuple;
        DataPageInfo dpinfo = new DataPageInfo();

        currentDataPageRid = currentDirPage.firstRecord();
        dpinfo = currentDirPage.getDatapageInfo(currentDataPageRid);
        PageId currentDatapageId = dpinfo.getPageId();

        while (currentDatapageId.pid != INVALID_PAGE) {
            pinPage(currentDatapageId, currentDataPage, false);

            System.out.println("PREV PAGE IS: " + currentDataPage.getPrevPage().pid);
            System.out.println("MIN VALUE IN PAGE: " + dpinfo.getPageId().pid + " IS: " + currentDataPage.getMinVal());
//            currentDataPage.printAllValues();
            System.out.println("MAX VALUE IN PAGE: " + dpinfo.getPageId().pid + " IS: " + currentDataPage.getMaxVal());
            System.out.println("NEXT PAGE IS: " + currentDataPage.getNextPage().pid);

            PageId nextDatapageId = currentDataPage.getNextPage();
            unpinPage(currentDatapageId, false);
            currentDatapageId.pid = nextDatapageId.pid;
        }
    }
}
