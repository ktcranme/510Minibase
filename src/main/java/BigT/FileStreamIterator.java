package BigT;

import bufmgr.*;
import global.MID;
import heap.*;
import iterator.*;

import java.io.IOException;

public class FileStreamIterator implements DatafileIterator {
    private DatafileIterator inputIterator;
    private CondExpr[] OutputFilter;
    public FldSpec[] perm_mat;
    private boolean closeFlag;

    /**
     * constructor
     *
     * @param file_name  heapfile to be opened
     * @param outFilter  select expressions
     * @exception IOException         some I/O fault
     * @exception FileScanException   exception from this class
     * @exception InvalidRelation     invalid relation
     */
    public FileStreamIterator(DatafileIterator inputIterator, CondExpr[] outFilter)
            throws IOException, FileScanException, InvalidRelation {
        OutputFilter = outFilter;
        this.inputIterator = inputIterator;
        closeFlag = false;
    }

    @Override
    public Map getNext(MID rid) throws InvalidMapSizeException, InvalidTupleSizeException, InvalidSlotNumberException, IOException, HFBufMgrException, PageNotReadException, PageUnpinnedException, HashOperationException, PagePinnedException, BufferPoolExceededException, BufMgrException, InvalidFrameNumberException, ReplacerException, HashEntryNotFoundException, InvalidTypeException, PredEvalException, FieldNumberOutOfBoundException, UnknowAttrType {
        while (true) {
            Map map;
            if ((map = inputIterator.getNext(rid)) == null) {
                rid.pageNo.pid = -1;
                rid.slotNo = -1;
                return null;
            }

            if (PredEval.Eval(OutputFilter, map)) {
                return map;
            }
        }
    }

    @Override
    public void closestream() throws IOException, HFBufMgrException {
        if (!closeFlag) {
            inputIterator.closestream();
            closeFlag = true;
        }
    }
}
