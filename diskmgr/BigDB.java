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
    bigT bigt;
    int _type;

    public BigDB(int type)
    {
        _type = type;
    }

    public void initBigT(String name, int type) throws 
        IOException,
        HFDiskMgrException,
        HFBufMgrException,
        HFException,
        ConstructPageException,
        GetFileEntryException,
        AddFileEntryException 
    {
        bigt = new bigT(name, type);
    }

    public void insertMap(Map map) throws Exception
    {
        bigt.insertMap(map.getMapByteArray());
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