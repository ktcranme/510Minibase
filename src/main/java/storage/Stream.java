package storage;

import BigT.DatafileIterator;
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

public class Stream implements DatafileIterator {
    private MID nextMapId;
    private SmallMapPage currentDataPage;
    private SmallMapFile file;
    private Boolean sorted;
    private DataPageIterator itr;

    Boolean closed;

    public Stream(SmallMapFile file, Boolean sorted) throws InvalidTupleSizeException, HFBufMgrException, InvalidSlotNumberException, IOException, PagePinnedException, PageUnpinnedException, HashOperationException, BufferPoolExceededException, BufMgrException, InvalidFrameNumberException, PageNotReadException, ReplacerException, HashEntryNotFoundException {
        this.sorted = sorted;
        init(file);
    }

    private void init(SmallMapFile file) throws IOException, InvalidSlotNumberException, InvalidTupleSizeException, HFBufMgrException, PagePinnedException, PageUnpinnedException, HashOperationException, ReplacerException, BufferPoolExceededException, BufMgrException, PageNotReadException, InvalidFrameNumberException, HashEntryNotFoundException {
        this.file = file;
        this.itr = file.getDataPageIterator();
        this.currentDataPage = itr.getNext();
        closed = false;
        if (this.currentDataPage == null) {
            this.nextMapId = null;
            closestream();
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
        sorted = false;
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
        if (closed) return null;

        if (this.nextMapId == null) {
            try {
                unpinPage(this.currentDataPage.getCurPage(), false);
            } catch (HFBufMgrException e) {
                // do nothing
                // Maybe someone's calling delete and delete has unpinned and freed the page
                System.out.println("Please dont delete while a stream is open!");
            }
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
        if (closed) return;
        if (this.currentDataPage != null && this.currentDataPage.getCurPage().pid != -1) {
            unpinPage(this.currentDataPage.getCurPage(), false);
            this.currentDataPage = null;
        }
        try {
            this.itr.close();
        } catch (Exception e) {
            // Pass
        }
        closed = true;
    }
}
