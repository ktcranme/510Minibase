package BigT;

import bufmgr.*;
import global.MID;
import heap.*;
import iterator.PredEvalException;
import iterator.UnknowAttrType;

import java.io.IOException;

public interface DatafileIterator {
    public Map getNext(MID rid) throws InvalidMapSizeException, InvalidTupleSizeException, InvalidSlotNumberException, IOException, HFBufMgrException, PageNotReadException, PageUnpinnedException, HashOperationException, PagePinnedException, BufferPoolExceededException, BufMgrException, InvalidFrameNumberException, ReplacerException, HashEntryNotFoundException, InvalidTypeException, PredEvalException, FieldNumberOutOfBoundException, UnknowAttrType;
    public void closestream() throws IOException, HFBufMgrException;
}
