package storage;

import global.Convert;
import global.GlobalConst;
import global.PageId;
import heap.InvalidTupleSizeException;
import heap.Tuple;

import java.io.IOException;

public class SmallDataPageInfo implements GlobalConst {
    public int recct;
    PageId pageId = new PageId();
    public String primaryKey;

    /** auxiliary fields of DataPageInfo */
    public int size = 8;   // size of DataPageInfo object in bytes
    private byte [] data;  // a data buffer
    private int offset;

    public short[] offsets = {
            0, // reccnt
            4, // pageid
            8 // primary key
    };

    public PageId getPageId() {
        return pageId;
    }

    public void init(String primaryKey, Integer lengthOfKey) {
        this.size += lengthOfKey;
        this.primaryKey = primaryKey;
    }

    public SmallDataPageInfo(String primaryKey, Integer lengthOfKey) {
        init(primaryKey, lengthOfKey);
        data = new byte[this.size]; // size of datapageinfo
        recct = 0;
        pageId.pid = INVALID_PAGE;
        offset = 0;
    }

    public SmallDataPageInfo(byte[] array, Integer lengthOfKey) throws InvalidTupleSizeException, IOException {
        init(null, lengthOfKey);
        // need check _atuple size == this.size ?otherwise, throw new exception
        if (array.length != size){
            throw new InvalidTupleSizeException(null, "HEAPFILE: TUPLE SIZE ERROR");
        }

        data = array;
        this.offset = 0;

        recct = Convert.getIntValue(this.offset + offsets[0], data);
        pageId = new PageId();
        pageId.pid = Convert.getIntValue(this.offset + offsets[1], data);
        this.primaryKey = Convert.getStrValue(this.offset + this.offsets[2], data, lengthOfKey);
    }

    public SmallDataPageInfo(byte[] array, int offset, Integer lengthOfKey) throws IOException {
        init(null, lengthOfKey);
        data = array;
        this.offset = offset;

        recct = Convert.getIntValue(this.offset + offsets[0], data);
        pageId = new PageId();
        pageId.pid = Convert.getIntValue(this.offset + offsets[1], data);
        this.primaryKey = Convert.getStrValue(this.offset + this.offsets[2], data, lengthOfKey);
    }

    public int getLength() {
        return data.length;
    }

    public byte [] returnByteArray()
    {
        return data;
    }

    public SmallDataPageInfo(Tuple _atuple, String primaryKey, Integer lengthOfKey)
            throws InvalidTupleSizeException, IOException {
        init(primaryKey, lengthOfKey);
        // need check _atuple size == this.size ?otherwise, throw new exception
        if (_atuple.getLength() != size){
            throw new InvalidTupleSizeException(null, "HEAPFILE: TUPLE SIZE ERROR");
        }

        data = _atuple.returnTupleByteArray();
        offset = _atuple.getOffset();

        recct = Convert.getIntValue(this.offset + offsets[0], data);
        pageId = new PageId();
        pageId.pid = Convert.getIntValue(this.offset + offsets[1], data);
        this.primaryKey = Convert.getStrValue(this.offset + this.offsets[2], data, lengthOfKey);
    }

    public Tuple convertToTuple() throws IOException {

        // 1) write availspace, recct, pageId into data []
        Convert.setIntValue(recct, this.offset + offsets[0], data);
        Convert.setIntValue(pageId.pid, this.offset + offsets[1], data);
        Convert.setStrValue(primaryKey, this.offset + offsets[2], data);


        // 2) creat a Tuple object using this array
        Tuple atuple = new Tuple(data, offset, size);

        // 3) return tuple object
        return atuple;

    }

    public void flushToTuple() throws IOException {
        Convert.setIntValue(recct, this.offset + offsets[0], data);
        Convert.setIntValue(pageId.pid, this.offset + offsets[1], data);
        Convert.setStrValue(primaryKey, this.offset + offsets[2], data);
    }
}
