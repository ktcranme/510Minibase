package storage;

import bufmgr.*;
import global.AttrType;
import global.PageId;
import global.RID;
import global.SystemDefs;
import heap.InvalidTupleSizeException;
import heap.InvalidTypeException;
import heap.Tuple;
import index.IndexException;
import iterator.*;

import java.io.IOException;

public class SmallDirPageInfoIterator extends Iterator {
    Integer pkLength;
    SmallDirpage page;
    PageId pageId;
    RID recid;
    Boolean closed;
    AttrType[] attrType = {
            new AttrType(AttrType.attrInteger),
            new AttrType(AttrType.attrInteger),
            new AttrType(AttrType.attrString)
    };

    public SmallDirPageInfoIterator(PageId firstPage, Integer pkLength) throws IOException, InvalidFrameNumberException, PageUnpinnedException, HashOperationException, PagePinnedException, BufferPoolExceededException, BufMgrException, PageNotReadException, ReplacerException {
        this.pageId = new PageId(firstPage.pid);
        this.pkLength = pkLength;

        page = new SmallDirpage();
        SystemDefs.JavabaseBM.pinPage(this.pageId, page, false);
        recid = page.firstRecord();
        closed = false;
    }

    @Override
    public Tuple get_next() throws IOException, JoinsException, IndexException, InvalidTupleSizeException, InvalidTypeException, PageNotReadException, TupleUtilsException, PredEvalException, SortException, LowMemException, UnknowAttrType, UnknownKeyTypeException, Exception {
        if (closed) return null;

        if (recid == null) {
            pageId.pid = page.getNextPage().pid;
            if (pageId.pid == -1) {
                close();
                return null;
            }
            SystemDefs.JavabaseBM.unpinPage(page.getCurPage(), false);
            SystemDefs.JavabaseBM.pinPage(pageId, page, false);
            recid = page.firstRecord();
            if (recid == null) {
                close();
                return null;
            }
        }

        SmallDataPageInfo info = page.getDatapageInfo(recid, pkLength);
        byte[] data = new byte[42];
        System.arraycopy(info.returnByteArray(), 0, data, 10, info.size);
        Tuple tuple = new Tuple(data, 0, data.length);
        tuple.setHdr((short) 3, attrType, new short[] { pkLength.shortValue() });

        recid = page.nextRecord(recid);

        return tuple;
    }

    @Override
    public void close() throws IOException, JoinsException, SortException, IndexException {
        if (closed) return;
        if (page != null && page.getCurPage().pid == -1) {
            try {
                SystemDefs.JavabaseBM.unpinPage(page.getCurPage(), false);
            } catch (Exception e) {
                e.printStackTrace();
                throw new IOException();
            }
        }
    }
}
