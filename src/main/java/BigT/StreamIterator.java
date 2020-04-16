package BigT;

import bufmgr.PageNotReadException;
import global.MID;
import heap.HFBufMgrException;
import heap.InvalidTypeException;
import index.IndexException;
import iterator.*;

import java.io.IOException;

public class StreamIterator extends Iterator {
    DatafileIterator itr;
    MID mid;

    public StreamIterator(DatafileIterator i) {
        itr = i;
        mid = new MID();
    }

    @Override
    public Map get_next() throws IOException, IndexException, InvalidTypeException, PageNotReadException, PredEvalException, SortException, LowMemException, UnknowAttrType, UnknownKeyTypeException, Exception {
        return itr.getNext(mid);
    }

    @Override
    public void close() throws IOException, JoinsException, SortException, IndexException {
        try {
            itr.closestream();
        } catch (HFBufMgrException e) {
            e.printStackTrace();
        }
    }
}
