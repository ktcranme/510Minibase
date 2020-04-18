package BigT;

import bufmgr.*;
import global.MID;
import heap.HFBufMgrException;
import heap.InvalidSlotNumberException;
import heap.InvalidTupleSizeException;
import heap.InvalidTypeException;
import index.IndexException;
import iterator.*;
import storage.BigT;
import storage.SmallMapFile;
import storage.StorageType;

import java.io.IOException;

public class MultiTypeFileStream extends Iterator {

    private BigT bigt;
    private CondExpr[] outF;
    FileStreamIterator fs;
    int count_file;

    public MultiTypeFileStream(BigT b, CondExpr[] out)
            throws IOException, FileScanException, InvalidRelation, HFBufMgrException {
        try {
            bigt = b;
            outF = out;
            count_file = 0;
            switch_file();
        } catch (Exception e){
            System.out.println("Error while initializing the MultiTypeFileStream");
            e.printStackTrace();
        }
    }

    public Map get_next() throws IOException, IndexException, InvalidTypeException, PageNotReadException, PredEvalException, SortException, LowMemException, UnknowAttrType, UnknownKeyTypeException, Exception {
        Map tm = null;
        if(count_file>5) {
            return null;
        }
        while((tm = fs.getNext(new MID()))!=null){
            return tm;
        }
        switch_file();
        while(count_file<=5){
            if((tm = fs.getNext(new MID()))!=null) {
                return tm;
            } else {
                switch_file();
            }
        }
        return null;
    }

    private void switch_file() throws FileScanException, IOException, InvalidRelation, HFBufMgrException, InvalidTupleSizeException, PagePinnedException, PageUnpinnedException, HashOperationException, BufferPoolExceededException, BufMgrException, InvalidFrameNumberException, InvalidSlotNumberException, PageNotReadException, ReplacerException, HashEntryNotFoundException, InvalidMapSizeException {
        if(count_file!=0){
            fs.closestream();
        }

        if(count_file<5) {
            if (StorageType.values()[count_file] == StorageType.TYPE_0) {
                fs = new FileStreamIterator(((VMapfile) bigt.storageTypes.get(StorageType.values()[count_file])).openStream(), outF);
            } else {
                fs = new FileStreamIterator(((SmallMapFile) bigt.storageTypes.get(StorageType.values()[count_file])).openStream(), outF);
            }
        }
        count_file++;
    }

    public void close() throws IOException {
        try {
            fs.closestream();
        } catch (HFBufMgrException e) {
            e.printStackTrace();
        }
    }
}
