package driver;

import btree.AddFileEntryException;
import btree.ConstructPageException;
import btree.GetFileEntryException;
import heap.HFBufMgrException;
import heap.HFDiskMgrException;
import heap.HFException;
import heap.Heapfile;
import storage.BigT;

import java.io.IOException;
import java.util.HashMap;

public class BigDB {
    HashMap<String, BigT> BigTables;

    public BigDB() {
        BigTables = new HashMap<>();
    }

    public BigT getBigT(String name) throws HFDiskMgrException, HFException, IOException, ConstructPageException, AddFileEntryException, GetFileEntryException, HFBufMgrException {
        //if the table already exists, return it
        if(BigTables.containsKey(name)) {
            return BigTables.get(name);
        }
        //otherwise, create one and return it
        else {
            BigT newBigT = new BigT(name);
            BigTables.put(name, newBigT);

            //we must check if this bigT already has some files on the disk
            //and then we just give it a reference



            return newBigT;
        }
    }
}
