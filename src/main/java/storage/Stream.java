package storage;

import BigT.Iterator;
import BigT.Map;
import bufmgr.*;
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
    private DataPageIterator itr;

    public Stream(SmallMapFile file, Boolean sorted) throws InvalidTupleSizeException, HFBufMgrException, InvalidSlotNumberException, IOException, PagePinnedException, PageUnpinnedException, HashOperationException, BufferPoolExceededException, BufMgrException, InvalidFrameNumberException, PageNotReadException, ReplacerException, HashEntryNotFoundException {
        this.sorted = sorted;
        init(file);
    }

    private void init(SmallMapFile file) throws IOException, InvalidSlotNumberException, InvalidTupleSizeException, HFBufMgrException, PagePinnedException, PageUnpinnedException, HashOperationException, ReplacerException, BufferPoolExceededException, BufMgrException, PageNotReadException, InvalidFrameNumberException, HashEntryNotFoundException {
        this.file = file;
        this.itr = file.getDataPageIterator();
        this.currentDataPage = itr.getNext();
        if (this.currentDataPage == null) {
            this.nextMapId = null;
        } else {
            if (this.sorted) {
                this.currentDataPage.sort(file.secondaryKey);
                this.nextMapId = this.currentDataPage.firstSorted();
            } else {
                this.nextMapId = this.currentDataPage.firstMap();
            }
        }
    }

    public Stream(SmallMapFile file) throws HFBufMgrException, IOException, InvalidTupleSizeException, InvalidSlotNumberException, PagePinnedException, PageUnpinnedException, HashOperationException, BufferPoolExceededException, BufMgrException, InvalidFrameNumberException, PageNotReadException, ReplacerException, HashEntryNotFoundException {
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

    public Map getNext(MID rid) throws IOException, InvalidSlotNumberException, HFBufMgrException, PageNotReadException, PageUnpinnedException, HashOperationException, PagePinnedException, BufferPoolExceededException, BufMgrException, InvalidFrameNumberException, InvalidTupleSizeException, ReplacerException, HashEntryNotFoundException {
        if (this.nextMapId == null) {
            unpinPage(this.currentDataPage.getCurPage(), false);
            this.currentDataPage = itr.getNext();
            if (this.currentDataPage == null)
                return null;

            if (this.sorted) {
                this.currentDataPage.sort(file.secondaryKey);
                this.nextMapId = this.currentDataPage.firstSorted();
            } else {
                this.nextMapId = this.currentDataPage.firstMap();
            }
        }

        Map map = this.currentDataPage.getMap(this.nextMapId, file.primaryKey);
        rid.slotNo = this.nextMapId.slotNo;
        rid.pageNo.pid = this.nextMapId.pageNo.pid;

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
