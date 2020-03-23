package diskmgr;

import java.io.IOException;

import BigT.*;
import btree.AddFileEntryException;
import btree.ConstructPageException;
import btree.GetFileEntryException;
import heap.HFBufMgrException;
import heap.HFDiskMgrException;
import heap.HFException;
import heap.InvalidSlotNumberException;
import heap.InvalidTupleSizeException;

public class BigDB extends DB
{
    public bigT bigt;
    int _type;

    public BigDB(int type)
    {
        _type = type;
    }

    public void initBigT(String name) throws 
        IOException,
        HFDiskMgrException,
        HFBufMgrException,
        HFException,
        ConstructPageException,
        GetFileEntryException,
        AddFileEntryException 
    {
        bigt = new bigT(name, _type);
    }

    public void insertMap(byte[] mapbytearray) throws Exception
    {
        bigt.insertMap(mapbytearray);
    }

    public int getMapCnt()throws HFBufMgrException,
        IOException,
        HFDiskMgrException,
        InvalidSlotNumberException,
        InvalidTupleSizeException 
    {
        return bigt.getMapCnt();
    }
} 