package storage;

import BigT.Iterator;
import BigT.Map;
import bufmgr.PageNotReadException;
import diskmgr.Page;
import global.MID;
import global.PageId;
import global.RID;
import global.SystemDefs;
import heap.*;
import index.IndexException;
import iterator.*;

import java.io.IOException;

public class Stream {
    private MID nextMapId;
    private SmallMapPage currentDataPage;
    private SmallMapFile file;
    private Boolean sorted;

    public Stream(SmallMapFile file, Boolean sorted) throws InvalidTupleSizeException, HFBufMgrException, InvalidSlotNumberException, IOException {
        this.sorted = sorted;
        init(file);
    }

    private void init(SmallMapFile file) throws IOException, InvalidSlotNumberException, InvalidTupleSizeException, HFBufMgrException {
        SmallDirpage currentDirPage = new SmallDirpage();
        PageId currentDirPageId = new PageId(file.getFirstDirPageId().pid);
        pinPage(currentDirPageId, currentDirPage, false/* Rdisk */);

        this.file = file;
        RID currentDataPageRid = currentDirPage.firstRecord();
        if (currentDataPageRid == null) {
            this.currentDataPage = null;
            this.nextMapId = null;
        } else {
            SmallDataPageInfo dpinfo = currentDirPage.getDatapageInfo(currentDataPageRid, file.ignoredLabel, file.ignoredLabel.length() * 2);
            PageId currentDatapageId = dpinfo.getPageId();

            this.currentDataPage = file.getNewDataPage();
            pinPage(currentDatapageId, this.currentDataPage, false);
            unpinPage(currentDirPageId, false);

            if (this.sorted) {
                this.currentDataPage.sort(file.secondaryKey);
                this.nextMapId = this.currentDataPage.firstSorted();
            } else {
                this.nextMapId = this.currentDataPage.firstMap();
            }
        }
    }

    public Stream(SmallMapFile file) throws HFBufMgrException, IOException, InvalidTupleSizeException, InvalidSlotNumberException {
        init(file);
    }

    private void pinPage(PageId pageno, Page page, boolean emptyPage) throws HFBufMgrException {
        try {
            SystemDefs.JavabaseBM.pinPage(pageno, page, emptyPage);
        } catch (Exception e) {
            throw new HFBufMgrException(e,"Stream.java: pinPage() failed");
        }

    }

    private void unpinPage(PageId pageno, boolean dirty) throws HFBufMgrException {
        try {
            SystemDefs.JavabaseBM.unpinPage(pageno, dirty);
        } catch (Exception e) {
            throw new HFBufMgrException(e,"Stream.java: unpinPage() failed");
        }
    }

    public Map getNext(MID rid) throws IOException, InvalidSlotNumberException, HFBufMgrException {
        if (this.nextMapId == null) {
            if (this.currentDataPage == null) {
                return null;
            }

            PageId nextPage = this.currentDataPage.getNextPage();
            unpinPage(this.currentDataPage.getCurPage(), false);
            this.currentDataPage = file.getNewDataPage();

            if (nextPage.pid != -1) {
                pinPage(nextPage, this.currentDataPage, false);
                if (this.sorted) {
                    this.currentDataPage.sort(file.secondaryKey);
                    this.nextMapId = this.currentDataPage.firstSorted();
                } else {
                    this.nextMapId = this.currentDataPage.firstMap();
                }
            } else {
                this.currentDataPage = null;
                return null;
            }
        }

        Map map = this.currentDataPage.getMap(this.nextMapId);

        if (this.sorted)
            this.nextMapId = this.currentDataPage.nextSorted(this.nextMapId);
        else
            this.nextMapId = this.currentDataPage.nextMap(this.nextMapId);

        return map;
    }

    public void closestream() throws IOException, HFBufMgrException {
        if (this.currentDataPage != null && this.currentDataPage.getCurPage().pid != -1) {
            unpinPage(this.currentDataPage.getCurPage(), false);
            this.currentDataPage = null;
        }
    }
}
