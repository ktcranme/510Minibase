package storage;

import bufmgr.*;
import global.PageId;
import global.RID;
import global.SystemDefs;
import heap.InvalidSlotNumberException;
import heap.InvalidTupleSizeException;

import java.io.IOException;

public class DataPageIterator {
    PageId dirpageId;
    PageId datapageId;
    Integer pkLength;
    SmallDirpage dirpage;
    RID datapageRid;

    Boolean closed;

    public DataPageIterator(PageId firstDirPage, Integer pkLength) throws IOException, PagePinnedException, HashOperationException, PageUnpinnedException, InvalidFrameNumberException, BufferPoolExceededException, BufMgrException, PageNotReadException, ReplacerException, HashEntryNotFoundException, InvalidTupleSizeException, InvalidSlotNumberException {
        this.dirpageId = new PageId(firstDirPage.pid);
        this.datapageId = new PageId(-1);
        this.pkLength = pkLength;
        this.closed = false;

        init();
    }

    private void init() throws IOException, InvalidFrameNumberException, PageUnpinnedException, HashOperationException, PagePinnedException, BufferPoolExceededException, BufMgrException, PageNotReadException, ReplacerException, HashEntryNotFoundException, InvalidTupleSizeException, InvalidSlotNumberException {
        dirpage = new SmallDirpage();
        SystemDefs.JavabaseBM.pinPage(dirpageId, dirpage, false);
        datapageRid = dirpage.firstRecord();
        if (datapageRid == null) {
            close();
        } else {
            SmallDataPageInfo dataPageInfo = dirpage.getDatapageInfo(datapageRid, pkLength);
            datapageId = new PageId(dataPageInfo.pageId.pid);
        }
    }

    public void close() throws IOException, PageUnpinnedException, InvalidFrameNumberException, HashEntryNotFoundException, ReplacerException {
        if (closed) return;
        if (dirpage.getCurPage().pid != -1) {
            try {
                SystemDefs.JavabaseBM.unpinPage(dirpage.getCurPage(), false);
            } catch (HashEntryNotFoundException e) {
                // do nothing
                // maybe this dirpage has been freed due to delete
            }
        }
        closed = true;
    }

    private void loadNextDirectory() throws IOException, PageUnpinnedException, ReplacerException, BufferPoolExceededException, HashOperationException, PageNotReadException, BufMgrException, InvalidFrameNumberException, PagePinnedException, HashEntryNotFoundException {
        PageId nextPage = new PageId(dirpage.getNextPage().pid);
        if (nextPage.pid == -1) {
            datapageRid = null;
            dirpageId = new PageId(-1);
            return;
        }

        SystemDefs.JavabaseBM.unpinPage(dirpageId, false);
        SystemDefs.JavabaseBM.pinPage(nextPage, dirpage, false);
        dirpageId.pid = nextPage.pid;
        datapageRid = dirpage.firstRecord();
    }

    private void getNextDatapageRid() throws IOException, PageNotReadException, PageUnpinnedException, HashOperationException, PagePinnedException, BufferPoolExceededException, BufMgrException, InvalidFrameNumberException, ReplacerException, HashEntryNotFoundException {
        datapageRid = dirpage.nextRecord(datapageRid);
        if (datapageRid == null) {
            loadNextDirectory();
        }
    }

    private void loadNextDatapageId() throws IOException, PageNotReadException, PageUnpinnedException, HashOperationException, PagePinnedException, BufferPoolExceededException, BufMgrException, InvalidFrameNumberException, HashEntryNotFoundException, ReplacerException, InvalidTupleSizeException, InvalidSlotNumberException {
        getNextDatapageRid();
        if (datapageRid == null) return;

        SmallDataPageInfo dataPageInfo = dirpage.getDatapageInfo(datapageRid, pkLength);
        datapageId = new PageId(dataPageInfo.pageId.pid);
    }

    public SmallMapPage getNext() throws InvalidSlotNumberException, InvalidTupleSizeException, IOException, PageUnpinnedException, ReplacerException, BufferPoolExceededException, HashOperationException, PageNotReadException, BufMgrException, InvalidFrameNumberException, PagePinnedException, HashEntryNotFoundException {
        if (closed) return null;

        if (datapageId.pid == -1) {
            loadNextDatapageId();
            if (datapageId.pid == -1) {
                close();
                return null;
            }
        }

        SmallMapPage page = new SmallMapPage(pkLength);
        SystemDefs.JavabaseBM.pinPage(datapageId, page, false);

        datapageId = new PageId(page.getNextPage().pid);
        return page;
    }
}