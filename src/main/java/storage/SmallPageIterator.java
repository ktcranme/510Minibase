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

public class SmallPageIterator implements DatafileIterator {
    SmallMapPage page;
    Integer primaryKey;
    Integer pkLength;
    Integer secondaryKey;
    MID currentMapId;

    Boolean closed;

    public SmallPageIterator(PageId pageId, Integer primaryKey, Integer pkLength, Integer secondaryKey) throws IOException, HFBufMgrException, PageUnpinnedException, HashOperationException, PagePinnedException, BufferPoolExceededException, BufMgrException, InvalidFrameNumberException, PageNotReadException, ReplacerException, InvalidSlotNumberException {
        this.page = new SmallMapPage(pkLength);
        this.primaryKey = primaryKey;
        this.pkLength = pkLength;
        this.secondaryKey = secondaryKey;

        closed = false;
        init(pageId);
    }

    private void init(PageId pageId) throws IOException, HFBufMgrException, PageUnpinnedException, ReplacerException, BufferPoolExceededException, HashOperationException, PageNotReadException, BufMgrException, InvalidFrameNumberException, PagePinnedException, InvalidSlotNumberException {
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

    @Override
    public Map getNext(MID rid) throws InvalidMapSizeException, InvalidTupleSizeException, InvalidSlotNumberException, IOException, HFBufMgrException, PageNotReadException, PageUnpinnedException, HashOperationException, PagePinnedException, BufferPoolExceededException, BufMgrException, InvalidFrameNumberException, ReplacerException, HashEntryNotFoundException, InvalidTypeException, PredEvalException, FieldNumberOutOfBoundException, UnknowAttrType {
        if (closed) return null;

        if (currentMapId == null) {
            closestream();
            return null;
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

        if (page != null && page.getCurPage().pid != -1) {
            try {
                SystemDefs.JavabaseBM.unpinPage(page.getCurPage(), false);
            } catch (Exception e) {
                // Pass
            }
            page = null;
        }

        closed = true;
    }
}
