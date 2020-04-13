package storage;

import global.Convert;
import global.PageId;
import global.RID;
import heap.*;

import java.io.IOException;

public class SmallDirpage extends Dirpage {
    public SmallDataPageInfo getDatapageInfo(RID rid, Integer lengthOfKey) throws InvalidSlotNumberException, IOException,
            InvalidTupleSizeException {
        return new SmallDataPageInfo(getRecord(rid), lengthOfKey);
    }

    public void replaceData(SmallDirpage anotherDir) throws IOException {
        PageId curPageId = new PageId(getCurPage().pid);
        PageId nextPageId = new PageId(getNextPage().pid);
        System.arraycopy(anotherDir.getpage(), 0, data, 0, MINIBASE_PAGESIZE);
        setCurPage(curPageId);
        setNextPage(nextPageId);
    }

    public boolean isEmpty() throws IOException {
        short usedPtr = Convert.getShortValue(USED_PTR, data);
        return usedPtr == MAX_SPACE;
    }

    public SmallDataPageInfo returnDatapageInfo(RID rid, Integer lengthOfKey) throws IOException, InvalidSlotNumberException,
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
            return new SmallDataPageInfo(data, offset, lengthOfKey);
        }

        else {
            throw new InvalidSlotNumberException(null, "HEAPFILE: INVALID_SLOTNO");
        }
    }
}
