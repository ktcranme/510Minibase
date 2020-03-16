package BigT;

import java.io.IOException;

import bufmgr.PageNotReadException;
import heap.InvalidTypeException;
import index.IndexException;
import iterator.JoinsException;
import iterator.LowMemException;
import iterator.PredEvalException;
import iterator.SortException;
import iterator.UnknowAttrType;
import iterator.UnknownKeyTypeException;

public class MapIter extends Iterator {
    Map[] map_ref;
    int curr_pos;

    public MapIter(Map[] map_ref) {
        this.map_ref = map_ref;
        this.curr_pos = 0;
        this.closeFlag = false;
    }

    @Override
    public Map get_next() throws IOException, IndexException, InvalidTypeException, PageNotReadException,
            PredEvalException, SortException, LowMemException, UnknowAttrType, UnknownKeyTypeException, Exception {

        if (this.closeFlag || curr_pos >= this.map_ref.length)
            return null;

        return this.map_ref[curr_pos];
    }

    @Override
    public void close() throws IOException, JoinsException, SortException, IndexException {
        this.closeFlag = true;
    }

}