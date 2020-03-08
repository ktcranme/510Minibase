package BigT;

import heap.*;
import iterator.CondExpr;
import iterator.FileScanException;
import iterator.FldSpec;
import iterator.InvalidRelation;
import iterator.PredEvalException;
import iterator.UnknowAttrType;
import iterator.WrongPermat;
import global.*;
import bufmgr.*;

import java.io.*;

/**
 * open a heapfile and according to the condition expression to get output file,
 * call get_next to get all tuples
 */
public class FileStream extends Iterator {
    private Heapfile f;
    private Stream stream;
    private CondExpr[] OutputFilter;
    public FldSpec[] perm_mat;
    private Map map;

    /**
     * constructor
     * 
     * @param file_name  heapfile to be opened
     * @param outFilter  select expressions
     * @exception IOException         some I/O fault
     * @exception FileScanException   exception from this class
     * @exception InvalidRelation     invalid relation
     */
    public FileStream(String file_name, CondExpr[] outFilter)
            throws IOException, FileScanException, InvalidRelation {
        OutputFilter = outFilter;

        try {
            f = new Heapfile(file_name);
        } catch (Exception e) {
            throw new FileScanException(e, "Create new heapfile failed");
        }

        try {
            stream = f.openStream();
        } catch (Exception e) {
            throw new FileScanException(e, "openScan() failed");
        }
    }

    /**
     * @return the result tuple
     * @throws InvalidMapSizeException
     * @throws InvalidTupleSizeException
     */
    public Map get_next()
            throws IOException, InvalidTypeException, PageNotReadException, PredEvalException, UnknowAttrType,
            FieldNumberOutOfBoundException, WrongPermat, InvalidMapSizeException, InvalidTupleSizeException {
        MID rid = new MID();

        while (true) {
            if ((map = stream.getNext(rid)) == null) {
                return null;
            }

            if (PredEval.Eval(OutputFilter, map) == true) {
                return map;
            }
        }
    }

    /**
     * implement the abstract method close() from super class Iterator to finish
     * cleaning up
     */
    public void close() {
        if (!closeFlag) {
            stream.closestream();
            closeFlag = true;
        }
    }

}
