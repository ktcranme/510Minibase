package heap;

import java.io.IOException;

import global.RID;

public class Tuplefile extends Heapfile {

    public Tuplefile(String name) throws HFException, HFBufMgrException, HFDiskMgrException, IOException {
        super(name);
    }

    public int getTupleCnt() throws InvalidSlotNumberException, InvalidTupleSizeException, HFDiskMgrException,
            HFBufMgrException, IOException {
        return getRecCnt();
    }

    public Tuple getTuple(RID rid) throws InvalidSlotNumberException, InvalidTupleSizeException, HFException,
            HFDiskMgrException, HFBufMgrException, Exception {
        byte[] rec = getRecord(rid);
        return new Tuple(rec, 0, rec.length);
    }

    public boolean updateTuple(RID rid, Tuple newtuple) throws InvalidSlotNumberException, InvalidUpdateException,
            InvalidTupleSizeException, HFException, HFDiskMgrException, HFBufMgrException, Exception {
        return updateRecord(rid, newtuple.getTupleByteArray());
    }

    public boolean deleteTuple(RID rid) throws InvalidSlotNumberException, InvalidTupleSizeException, HFException,
            HFBufMgrException, HFDiskMgrException, Exception {
        return deleteRecord(rid);
    }

    public RID insertTuple(Tuple tuple) throws InvalidSlotNumberException, InvalidTupleSizeException,
            SpaceNotAvailableException, HFException, HFBufMgrException, HFDiskMgrException, IOException {
        return insertRecord(tuple.getTupleByteArray());
    }

    @Override
    public Scan openScan() throws InvalidTupleSizeException, IOException {
        return new Scan(this);
    }
}