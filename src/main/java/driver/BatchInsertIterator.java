package driver;

import BigT.*;
import BigT.Iterator;
import bufmgr.PageNotReadException;
import global.MID;
import heap.*;
import index.IndexException;
import iterator.*;
import storage.StorageType;
import storage.Stream;

import java.io.IOException;

public class BatchInsertIterator extends Iterator{
    DatafileIterator _fileItr;
    Iterator _csvItr;
    boolean finishedWithCSV;
    MID rid;

    public BatchInsertIterator(Iterator csvItr, DatafileIterator fileItr) throws HFBufMgrException, IOException, InvalidMapSizeException, InvalidSlotNumberException, InvalidTupleSizeException {
        _csvItr = csvItr;
        _fileItr = fileItr;
        finishedWithCSV = false;
        rid = new MID();
    }

    public Map get_next() throws IOException, IndexException, InvalidTypeException, PageNotReadException, PredEvalException, SortException, LowMemException, UnknowAttrType, UnknownKeyTypeException, Exception {
        if(!finishedWithCSV) {
            Map tempMap = _csvItr.get_next();
            if (tempMap != null) {
                return tempMap;
            } else {
                finishedWithCSV = true;
            }
        }
        return _fileItr.getNext(rid);
    }

    @Override
    public void close() throws IOException, JoinsException, SortException, IndexException {
        _csvItr.close();
        try {
            _fileItr.closestream();
        } catch (HFBufMgrException e) {
            e.printStackTrace();
        }
    }
}
