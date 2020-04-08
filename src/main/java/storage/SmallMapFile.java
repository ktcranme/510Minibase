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
        newPage.setCurPage(newPageId);
        newPage.setPrevPage(new PageId(-1));
        newPage.setNextPage(new PageId(-1));

        newPage.init(newPageId, apage);

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

        currentPage.setNextPage(newPageId);
        newPage.setPrevPage(currentPageId);
        newPage.setCurPage(newPageId);
        newPage.setNextPage(new PageId(-1));

        newPage.init(newPageId, apage);

        return newPage;
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
            curDataPageId = curDataPage.getCurPage();
        } else {
            dpinfo = dirpage.getDatapageInfo(currentDataPageRid);
            pinPage(dpinfo.getPageId(), curDataPage, false);
            curDataPageId = dpinfo.getPageId();
        }

        unpinPage(dirPageId, currentDataPageRid == null);

        // At this point, only curDataPage is pinned

        while (curDataPageId.pid != INVALID_PAGE) {
            nextDataPageId = curDataPage.getNextPage();

            if (curDataPage.available_space() < HFPage.SIZE_OF_SLOT + SmallMap.map_size) {
                if (nextDataPageId.pid == INVALID_PAGE) {
                    // Create a new page
                    nextDataPage = makeNextDataPage(curDataPage, nextDataPageId);
                    unpinPage(curDataPageId, true);
                } else {
                    unpinPage(curDataPageId, false);
                    pinPage(nextDataPageId, nextDataPage, false);
                }

                curDataPage = nextDataPage;
                curDataPageId = new PageId(nextDataPageId.pid);
                nextDataPage = getNewDataPage();
                nextDataPageId = new PageId();
                continue;
            }

            break;
        }

        RID res = curDataPage.insertRecord(map.getMapByteArray());
        unpinPage(curDataPageId, true);

        return res;
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
            currentDataPage.printAllValues();
            System.out.println("MAX VALUE IN PAGE: " + dpinfo.getPageId().pid + " IS: " + currentDataPage.getMaxVal());
            System.out.println("NEXT PAGE IS: " + currentDataPage.getNextPage().pid);

            PageId nextDatapageId = currentDataPage.getNextPage();
            unpinPage(currentDatapageId, false);
            currentDatapageId.pid = nextDatapageId.pid;
        }
    }
}
