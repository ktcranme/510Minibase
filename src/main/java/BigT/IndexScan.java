package BigT;

import global.*;
import btree.*;
import index.IndexException;
import index.IndexUtils;
import index.UnknownIndexTypeException;
import iterator.*;
import heap.*;

import java.io.*;


/**
 * Index Scan iterator will directly access the required tuple using
 * the provided key. It will also perform selections and projections.
 * information about the tuples and the index are passed to the constructor,
 * then the user calls <code>get_next()</code> to get the tuples.
 */
public class IndexScan extends Iterator {

    /**
     * Cleaning up the index scan, does not remove either the original
     * relation or the index from the database.
     *
     * @throws IndexException error from the lower layer
     * @throws IOException    from the lower layer
     */
    public void close() throws IOException, IndexException {
        if (!closeFlag) {
            if (indScan instanceof BTFileScan) {
                try {
                    ((BTFileScan) indScan).DestroyBTreeFileScan();
                } catch (Exception e) {
                    throw new IndexException(e, "BTree error in destroying index scan.");
                }
            }

            closeFlag = true;
        }
    }


    /**
     * class constructor. set up the index scan for maps.
     *
     * @param index     type of the index (B_Index, Hash)
     * @param relName   name of the input relation
     * @param indName   name of the input index
     * @param selects   conditions to apply, first one is primary
     * @param indexOnly whether the answer requires only the key or the tuple
     * @throws IndexException            error from the lower layer
     * @throws InvalidTypeException      tuple type not valid
     * @throws InvalidTupleSizeException tuple size not valid
     * @throws UnknownIndexTypeException index type unknown
     * @throws IOException               from the lower layer
     */
    public IndexScan(
            IndexType index,
            final String relName,
            final String indName,
            CondExpr selects_index[],
            CondExpr selects[],
            final boolean indexOnly
    )
            throws IndexException,
            UnknownIndexTypeException {
        _selects = selects;
        _selects_index = selects_index;
        map1 = new Map();

        index_only = indexOnly;  // added by bingjie miao

        try {
            f = new Mapfile(relName);
        } catch (Exception e) {
            throw new IndexException(e, "IndexScan.java: Heapfile not created");
        }

        switch (index.indexType) {
            // linear hashing is not yet implemented
            case IndexType.B_Index:
                // error check the select condition
                // must be of the type: value op symbol || symbol op value
                // but not symbol op symbol || value op value
                try {
                    indFile = new BTreeFile(indName);
                } catch (Exception e) {
                    throw new IndexException(e, "IndexScan.java: BTreeFile exceptions caught from BTreeFile constructor");
                }

                try {
                    indScan = (BTFileScan) IndexUtils.BTree_scan(_selects_index, indFile);
                } catch (Exception e) {
                    throw new IndexException(e, "IndexScan.java: BTreeFile exceptions caught from IndexUtils.BTree_scan().");
                }

                break;
            case IndexType.None:
            default:
                throw new UnknownIndexTypeException("Only BTree index is supported so far");

        }

    }

    /**
     * returns the next tuple.
     * if <code>index_only</code>, only returns the key value
     * (as the first field in a tuple)
     * otherwise, retrive the tuple and returns the whole tuple
     *
     * @return the tuple
     * @throws IndexException          error from the lower layer
     * @throws UnknownKeyTypeException key type unknown
     * @throws IOException             from the lower layer
     */
    public Map get_next()
            throws Exception {
        RID rid;
        KeyDataEntry nextentry = null;

        try {
            nextentry = indScan.get_next();
        } catch (Exception e) {
            throw new IndexException(e, "IndexScan.java: BTree error");
        }

        while (nextentry != null) {
            rid = ((LeafData) nextentry.data).getData();
            try {
                map1 = f.getMap(new MID(rid));
            } catch (Exception e) {
                throw new IndexException(e, "IndexScan.java: getRecord failed");
            }
            if (PredEval.Eval(_selects, map1)) {
                return map1;
            }
            try {
                nextentry = indScan.get_next();
            } catch (Exception e) {
                throw new IndexException(e, "IndexScan.java: BTree error");
            }
        }

        return null;
    }

    public FldSpec[] perm_mat;
    private IndexFile indFile;
    private IndexFileScan indScan;
    private CondExpr[] _selects;
    private CondExpr[] _selects_index;
    private Mapfile f;
    private Map map1;
    private boolean index_only;

}
