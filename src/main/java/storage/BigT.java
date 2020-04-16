package storage;

import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Random;

import BigT.VMapfile;
import BigT.Map;
import BigT.Iterator;
import btree.AddFileEntryException;
import btree.BTreeFile;
import btree.ConstructPageException;
import btree.GetFileEntryException;
import bufmgr.HashEntryNotFoundException;
import bufmgr.InvalidFrameNumberException;
import bufmgr.PageUnpinnedException;
import bufmgr.ReplacerException;
import global.AttrType;
import global.GlobalConst;
import global.MID;
import heap.*;

public class BigT {
    String name;
    HashMap<StorageType, Heapfile> storageTypes;
    HashMap<StorageType, BTreeFile> indexTypes;
    Random rand = new Random();

    public BigT(String name) {
        this.name = name;
        storageTypes = new HashMap<>();
    }

    private String generateBigTName(StorageType type) {
        return name + "_" + type.toString() + "_" + rand.nextInt(10000);
    }

    private String generateIndexName(StorageType type) {
        return name + "_" + type.toString() + "_idx" + "_" + rand.nextInt(10000);
    }

    private void createStorageOfType(StorageType type) throws HFException, HFDiskMgrException, HFBufMgrException, IOException, ConstructPageException, GetFileEntryException, AddFileEntryException {
        switch (type) {
            case TYPE_0:
                storageTypes.put(type, new VMapfile(generateBigTName(type)));
                break;
            case TYPE_1:
                storageTypes.put(type, new SmallMapFile(generateBigTName(type), 1, null, GlobalConst.MAXROWLABELSIZE));
                indexTypes.put(type, new BTreeFile(generateIndexName(type), AttrType.attrString, GlobalConst.MAXROWLABELSIZE, 1));
                break;
            case TYPE_2:
                storageTypes.put(type, new SmallMapFile(generateBigTName(type), 1, 3, GlobalConst.MAXROWLABELSIZE));
                indexTypes.put(type, new BTreeFile(generateIndexName(type), AttrType.attrString, GlobalConst.MAXROWLABELSIZE + GlobalConst.MAXVALUESIZE + 1, 1));
                break;
            case TYPE_3:
                storageTypes.put(type, new SmallMapFile(generateBigTName(type), 2, null, GlobalConst.MAXCOLUMNLABELSIZE));
                indexTypes.put(type, new BTreeFile(generateIndexName(type), AttrType.attrString, GlobalConst.MAXCOLUMNLABELSIZE, 1));
                break;
            case TYPE_4:
                storageTypes.put(type, new SmallMapFile(generateBigTName(type), 2, 1, GlobalConst.MAXCOLUMNLABELSIZE));
                indexTypes.put(type, new BTreeFile(generateIndexName(type), AttrType.attrString, GlobalConst.MAXCOLUMNLABELSIZE + GlobalConst.MAXROWLABELSIZE + 1, 1));
                break;
            default:
                throw new HFException(null, "Invalid Storage Type!");
        }
    }

    public void batchInsert(Iterator inputFile, StorageType type) throws HFBufMgrException, HFException, IOException, ConstructPageException, AddFileEntryException, GetFileEntryException, HFDiskMgrException {
        // Insert
        if (type == StorageType.TYPE_0) {
            VMapfile file = (VMapfile) storageTypes.remove(type);
            // Open stream from this, create a new file and insert into that
            createStorageOfType(type);
            VMapfile newFile = (VMapfile) storageTypes.get(type);
        }

        SmallMapFile file = (SmallMapFile) storageTypes.remove(type);
        // Open stream from this, create a new file and insert into that
        createStorageOfType(type);
        SmallMapFile newFile = (SmallMapFile) storageTypes.get(type);
    }

    public MID mapInsert(Map map, StorageType type) throws HFDiskMgrException, InvalidTupleSizeException, HFException, IOException, InvalidSlotNumberException, HFBufMgrException, SpaceNotAvailableException, ConstructPageException, GetFileEntryException, AddFileEntryException {
        if (!storageTypes.containsKey(type)) {
            // Create
            createStorageOfType(type);
        }

        // Insert
        if (type == StorageType.TYPE_0) {
            VMapfile file = (VMapfile) storageTypes.get(type);
            return file.insertMap(map);
        }

        // Probably use index here?
        // Maybe you should just let the file handle the sorting
        SmallMapFile file = (SmallMapFile) storageTypes.get(type);
        return file.insertMap(map);
    }

    public Iterator query(int orderType, String rowFilter, String columnFilter, String valueFilter) {
        // Could use indexes, or filescan with filterstream or whatever
        return null;
    }

    public Integer getRowCount() {
        // Unique rows
        return 0;
    }

    public Integer getColumnCount() {
        // Unique columns
        return 0;
    }

    public Integer getMapCount() {
        // Does this include all versions?
        return 0;
    }

    public BigT rowJoin(Integer amountOfMem, Iterator leftStream, String columnName) {
        // Need amt of mem here for sort?
        // I believe this is going to be across all the 5 types, so we dont need a RightBigTName as given in the specs

        // Create a new BigT instance too and return it to BigDB. Then BigDB could invoke queryAll on this new BigT

        return null;
    }

    public BigT rowSort(String columnName, Integer nPages) {
        // Create a new BigT instance too and return it to BigDB. Then BigDB could invoke queryAll on this new BigT

        return null;
    }

    public void close() {
        for (BTreeFile file : indexTypes.values()) {
            try {
                file.close();
            } catch (Exception e) {
                // Force close these guys
            }
        }
    }

}
