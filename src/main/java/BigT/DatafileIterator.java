package BigT;

import bufmgr.*;
import global.MID;
import heap.HFBufMgrException;
import heap.InvalidSlotNumberException;
import heap.InvalidTupleSizeException;

import java.io.IOException;

public interface DatafileIterator {
    public Map getNext(MID rid) throws InvalidMapSizeException, InvalidTupleSizeException, InvalidSlotNumberException, IOException, HFBufMgrException, PageNotReadException, PageUnpinnedException, HashOperationException, PagePinnedException, BufferPoolExceededException, BufMgrException, InvalidFrameNumberException, ReplacerException, HashEntryNotFoundException;
    public void closestream() throws IOException, HFBufMgrException;
}
