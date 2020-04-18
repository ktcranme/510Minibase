package storage;

import BigT.*;
import bufmgr.*;
import global.MID;
import global.PageId;
import global.SystemDefs;
import heap.*;
import iterator.PredEvalException;
import iterator.UnknowAttrType;

import java.io.IOException;

public class PrimaryIterator implements DatafileIterator {
    PageId pageId;
    SmallMapPage page;
    Boolean closed;
    MID currentMapId;
    Integer pkLength;
    Integer secondaryKey;
    Integer primaryKey;

    public PrimaryIterator(PageId startPage, Integer primaryKey, Integer pkLength, Integer secondaryKey) throws IOException, PagePinnedException, HashOperationException, PageUnpinnedException, InvalidFrameNumberException, BufferPoolExceededException, BufMgrException, PageNotReadException, ReplacerException, HFBufMgrException, InvalidSlotNumberException {
        pageId = new PageId(startPage.pid);
        page = new SmallMapPage(pkLength);
        closed = false;
        currentMapId = null;
        this.pkLength = pkLength;
        this.secondaryKey = secondaryKey;
        this.primaryKey = primaryKey;
        init();
    }

    private void init() throws IOException, InvalidFrameNumberException, PageUnpinnedException, HashOperationException, PagePinnedException, BufferPoolExceededException, BufMgrException, PageNotReadException, ReplacerException, HFBufMgrException, InvalidSlotNumberException {
        if (pageId.pid == -1) {
            closestream();
            return;
        }
        SystemDefs.JavabaseBM.pinPage(pageId, page, false);
        if (secondaryKey != null) {
            page.sort(secondaryKey);
            currentMapId = page.firstSorted();
        } else {
            currentMapId = page.firstMap();
        }
    }

    private void loadNextDataPage() throws IOException, PageUnpinnedException, InvalidFrameNumberException, HashEntryNotFoundException, ReplacerException, HFBufMgrException, BufMgrException, BufferPoolExceededException, PageNotReadException, PagePinnedException, HashOperationException {
        PageId nextPageId = new PageId(page.getNextPage().pid);
        if (nextPageId.pid == -1) {
            page = null;
            return;
        }
        SystemDefs.JavabaseBM.unpinPage(pageId, false);
        pageId.pid = nextPageId.pid;
        SystemDefs.JavabaseBM.pinPage(pageId, page, false);
        if (secondaryKey != null)
            page.sort(secondaryKey);
    }

    @Override
    public Map getNext(MID rid) throws InvalidMapSizeException, InvalidTupleSizeException, InvalidSlotNumberException, IOException, HFBufMgrException, PageNotReadException, PageUnpinnedException, HashOperationException, PagePinnedException, BufferPoolExceededException, BufMgrException, InvalidFrameNumberException, ReplacerException, HashEntryNotFoundException, InvalidTypeException, PredEvalException, FieldNumberOutOfBoundException, UnknowAttrType {
        if (closed) return null;

        if (currentMapId == null) {
            loadNextDataPage();
            if (page == null) {
                closestream();
                return null;
            }

            if (secondaryKey != null) {
                currentMapId = page.firstSorted();
            } else {
                currentMapId = page.firstMap();
            }

            if (currentMapId == null) {
                closestream();
                return null;
            }
        }

        Map map = page.getMap(currentMapId, primaryKey);

        if (secondaryKey != null) {
            currentMapId = page.nextSorted(currentMapId);
        } else {
            currentMapId = page.nextMap(currentMapId);
        }

        return map;
    }

    @Override
    public void closestream() throws IOException, HFBufMgrException {
        if (closed) return;
        if (pageId.pid != -1) {
            try {
                SystemDefs.JavabaseBM.unpinPage(pageId, false);
            } catch (Exception e) {
                // pass
            }
        }
        closed = true;
    }
}
