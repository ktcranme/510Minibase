/* File HFPage.java */

package heap;

import java.io.*;

import global.*;
import heap.HFPage;
import diskmgr.*;

public class Dirpage extends HFPage {
    public Dirpage(Page page) {
        super(page);
    }

    public Dirpage() {
        super();
    }

    public DataPageInfo getDatapageInfo(RID rid) throws InvalidSlotNumberException, IOException,
            InvalidTupleSizeException {
        return new DataPageInfo(getRecord(rid));
    }

    public DataPageInfo returnDatapageInfo(RID rid) throws IOException, InvalidSlotNumberException,
            InvalidTupleSizeException {
        short recLen;
        short offset;
        PageId pageNo = new PageId();
        pageNo.pid = rid.pageNo.pid;

        curPage.pid = Convert.getIntValue(CUR_PAGE, data);
        int slotNo = rid.slotNo;

        // length of record being returned
        recLen = getSlotLength(slotNo);
        int slotCnt = Convert.getShortValue(SLOT_CNT, data);

        if ((slotNo >= 0) && (slotNo < slotCnt) && (recLen > 0) && (pageNo.pid == curPage.pid)) {

            offset = getSlotOffset(slotNo);
            return new DataPageInfo(data, offset);
        }

        else {
            throw new InvalidSlotNumberException(null, "HEAPFILE: INVALID_SLOTNO");
        }
    }
}