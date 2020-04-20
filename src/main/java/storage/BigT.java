package storage;

import java.io.IOException;
import java.sql.SQLOutput;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Random;

import BigT.*;
import BigT.VMapfile;
import BigT.Map;
import BigT.Iterator;
import BigT.DatafileIterator;
import BigT.Sort;
import BigT.Stream;
import BigT.FileStreamIterator;
import BigT.CandidateForDeletion;
import BigT.StreamIterator;
import btree.*;
import bufmgr.*;
import driver.BatchInsertIterator;
import driver.FilterParser;
import global.*;
import heap.*;
import iterator.CondExpr;
import iterator.FileScanException;
import iterator.InvalidRelation;
import iterator.SortException;

import static global.GlobalConst.*;

public class BigT {
    String name;
    public HashMap<StorageType, Heapfile> storageTypes;
    public HashMap<StorageType, BTreeFile> indexTypes;
    Random rand = new Random();
    public final String DELIMITER = ",";
    AttrType[] attrType = {new AttrType(AttrType.attrString), new AttrType(AttrType.attrString), new AttrType(AttrType.attrInteger), new AttrType(AttrType.attrString)};
    short[] attrSize = {MAXROWLABELSIZE, MAXCOLUMNLABELSIZE, MAXVALUESIZE};

    public BigT(String name) throws HFBufMgrException, HFException, IOException, ConstructPageException, AddFileEntryException, GetFileEntryException, HFDiskMgrException {
        this.name = name;
        storageTypes = new HashMap<>();
        indexTypes = new HashMap<>();

        for (StorageType type : StorageType.values()) {
            createStorageOfType(type);
        }
    }

    public String getName() {
        return name;
    }

    private String generateBigTName(StorageType type) {
        return name + "_" + type.toString();
    }

    private String generateIndexName(StorageType type) {
        return name + "_" + type.toString() + "_idx";
    }

    private void createStorageOfType(StorageType type) throws HFException, HFDiskMgrException, HFBufMgrException, IOException, ConstructPageException, GetFileEntryException, AddFileEntryException {
        switch (type) {
            case TYPE_0:
                storageTypes.put(type, new VMapfile(generateBigTName(type)));
                break;
            case TYPE_1:
                storageTypes.put(type, new SmallMapFile(generateBigTName(type), 1, null, MAXROWLABELSIZE));
                indexTypes.put(type, new BTreeFile(generateIndexName(type), AttrType.attrString, MAXROWLABELSIZE, 1));
                break;
            case TYPE_2:
                storageTypes.put(type, new SmallMapFile(generateBigTName(type), 2, null, MAXCOLUMNLABELSIZE));
                indexTypes.put(type, new BTreeFile(generateIndexName(type), AttrType.attrString, MAXCOLUMNLABELSIZE, 1));
                break;
            case TYPE_3:
                storageTypes.put(type, new SmallMapFile(generateBigTName(type), 2, 1, MAXCOLUMNLABELSIZE));
                indexTypes.put(type, new BTreeFile(generateIndexName(type), AttrType.attrString, MAXCOLUMNLABELSIZE + MAXROWLABELSIZE + 1, 1));
                break;
            case TYPE_4:
                storageTypes.put(type, new SmallMapFile(generateBigTName(type), 1, 3, MAXROWLABELSIZE));
                indexTypes.put(type, new BTreeFile(generateIndexName(type), AttrType.attrString, MAXROWLABELSIZE + MAXVALUESIZE + 1, 1));
                break;
            default:
                throw new HFException(null, "Invalid Storage Type!");
        }
    }

    private void createFileOfType(StorageType type) throws HFException, HFDiskMgrException, HFBufMgrException, IOException, ConstructPageException, GetFileEntryException, AddFileEntryException {
        switch (type) {
            case TYPE_0:
                storageTypes.put(type, new VMapfile(generateBigTName(type)));
                break;
            case TYPE_1:
                storageTypes.put(type, new SmallMapFile(generateBigTName(type), 1, null, MAXROWLABELSIZE));
                break;
            case TYPE_2:
                storageTypes.put(type, new SmallMapFile(generateBigTName(type), 2, null, MAXCOLUMNLABELSIZE));
                break;
            case TYPE_3:
                storageTypes.put(type, new SmallMapFile(generateBigTName(type), 2, 1, MAXCOLUMNLABELSIZE));
                break;
            case TYPE_4:
                storageTypes.put(type, new SmallMapFile(generateBigTName(type), 1, 3, MAXROWLABELSIZE));
                break;
            default:
                throw new HFException(null, "Invalid Storage Type!");
        }
    }

    MID crIndexMapFind(Map findMap, BTreeFile index_f) throws IOException,
            PinPageException,
            IteratorException,
            KeyNotMatchException,
            UnpinPageException,
            ConstructPageException,
            ScanIteratorException, PageUnpinnedException, InvalidFrameNumberException, HashEntryNotFoundException, ReplacerException {
        BTFileScan bs = index_f.new_scan(new StringKey(findMap.getColumnLabel() + DELIMITER + findMap.getRowLabel()), new StringKey(findMap.getColumnLabel() + DELIMITER + findMap.getRowLabel()));
        KeyDataEntry ky;
        MID mid = null;
        if ((ky = bs.get_next()) != null) {
            mid = new MID(((LeafData) ky.data).getData());
            bs.DestroyBTreeFileScan();
            return mid;
        }
        bs.DestroyBTreeFileScan();
        return null;
    }

    public void batchInsert(Iterator inputFile, StorageType type, int numbuf) throws Exception {
        // Insert
        DatafileIterator dfItr;
        VMapfile tempFile = new VMapfile(generateBigTName(type) + "_temp");
        if (type == StorageType.TYPE_0) {
            VMapfile file = (VMapfile) storageTypes.get(type);
            dfItr = file.openStream();
        } else {
            SmallMapFile file = (SmallMapFile) storageTypes.get(type);
            dfItr = file.openSortedStream();
        }

        Iterator i = new BatchInsertIterator(inputFile, dfItr);
        AttrType[] attrType = {new AttrType(AttrType.attrString), new AttrType(AttrType.attrString), new AttrType(AttrType.attrInteger), new AttrType(AttrType.attrString)};
        short[] attrSize = {MAXROWLABELSIZE, MAXCOLUMNLABELSIZE, MAXVALUESIZE};
        //NOTE: this will sort either RCT or CRT in all cases - for case 5, a second sort is required to put it back in correct ordering.

        Mapfile tempMapFile = new Mapfile(generateBigTName(type) + "_phy_temp");
        BTreeFile tempbtf = new BTreeFile(generateBigTName(type) + "_idx_temp", AttrType.attrString, GlobalConst.MAXCOLUMNLABELSIZE + GlobalConst.MAXROWLABELSIZE + 1, 1);

        Map comboMap = i.get_next();
        Map delMap = new Map();
        while (comboMap != null) {
//            comboMap.print();
            MID tmpmid = crIndexMapFind(comboMap, tempbtf);

            if (tmpmid != null) {
                MID tm = tempMapFile.updateMap(tmpmid, comboMap, delMap);
//                if (tm == null)
//                    continue;
//                if (!tm.isReused) {
//                    tempbtf.insert(new StringKey(String.join(DELIMITER, comboMap.getColumnLabel(), comboMap.getRowLabel())), new RID(tm));
//                }
            } else {
                MID tm = tempMapFile.insertMap(comboMap);
                tempbtf.insert(new StringKey(String.join(DELIMITER, comboMap.getColumnLabel(), comboMap.getRowLabel())), new RID(tm));
            }
            comboMap = i.get_next();
        }
        i.close();
        inputFile.close();
        tempbtf.close();
        tempbtf.destroyFile();

        Stream itr = tempMapFile.openStream();
        MID tmid = new MID();
        Map tempMap = itr.getNext(tmid);
        String currMapRC = "";
        Map[] latestFromSort = new Map[3];
        while(tempMap != null){
            currMapRC = tempMap.getRowLabel() + tempMap.getColumnLabel();

            int c = 0;
            while(tempMap != null && currMapRC.equals(tempMap.getRowLabel() + tempMap.getColumnLabel())) {
                latestFromSort[c%3] = new Map();
                latestFromSort[c%3].mapCopy(tempMap); //Note: this only works since we sort in timestamp order at the end
                c++;

                tempMap = itr.getNext(tmid);
            }

            //put a list of all the versions - starting with the 3 latest from sort
            ArrayList<CandidateForDeletion> allVersions = new ArrayList<CandidateForDeletion>();
            for(int k = 0; k < 3; k++) {
                if (latestFromSort[k] != null) {
                    allVersions.add(new CandidateForDeletion(latestFromSort[k], null, null));
                }
            }

            //search for other versions of the map and add them to list
            for(StorageType t : StorageType.values()) {
                //don't need to check the type we're inserting into since
                //we sorted and grabbed the latest three with that type
                if(t != type) {
                    CondExpr[] filter = FilterParser.parseCombine(String.join("##", latestFromSort[0].getRowLabel(), latestFromSort[0].getColumnLabel(), "*"));
                    DatafileIterator fileToBeSearched;
                    if(t==StorageType.TYPE_0) {
                        fileToBeSearched = ((VMapfile)storageTypes.get(t)).openStream();
                    } else if (type == StorageType.TYPE_1 || type == StorageType.TYPE_4) {
                        fileToBeSearched = ((SmallMapFile)storageTypes.get(t)).getPrimaryStream(latestFromSort[0].getRowLabel(), true);
                    } else {
                        fileToBeSearched = ((SmallMapFile)storageTypes.get(t)).getPrimaryStream(latestFromSort[0].getColumnLabel(), true);
                    }
                    DatafileIterator searchIterator = new FileStreamIterator(fileToBeSearched, filter);

                    MID mid = new MID();
                    Map tempMapFromSearch = searchIterator.getNext(mid);
                    while(tempMapFromSearch != null) {
                        allVersions.add(new CandidateForDeletion(tempMapFromSearch, mid, t));

                        tempMapFromSearch = searchIterator.getNext(mid);
                    }

                    //cleanup
                    fileToBeSearched.closestream();
                    searchIterator.closestream();
                }
            }


            //AT THIS POINT we have the list of all versions of the map
            //if there are more than 3 total versions - we will either have to delete some
            //or ignore some from the latestFromSort array (or both)
            //loop through 3 times, selecting the highest timestamp values and set those not for deletion
            if(allVersions.size() > 3) {
                for (int k = 0; k < 3; k++) {
                    int maxTimeStamp = -1;
                    int indexOfMaxTimeStamp = -1;

                    for(int j = 0; j < allVersions.size(); j++){
                        if(allVersions.get(j).getMap().getTimeStamp() > maxTimeStamp && allVersions.get(j).getDeleteMe()) {
                            maxTimeStamp = allVersions.get(j).getMap().getTimeStamp();
                            indexOfMaxTimeStamp = j;
                        }
                    }

                    //mark this highest time stamp(that hasn't already been marked) to not be deleted
                    allVersions.get(indexOfMaxTimeStamp).setDeleteMe(false);
                }
            } else {
                //none are going to be deleted
                for(int k = 0; k < allVersions.size(); k++) {
                    allVersions.get(k).setDeleteMe(false);
                }
            }

            //AT THIS POINT we have the same array of candidates but now 3 are set to not be deleted
            //loop through and delete any that have DeleteMe == true (or if type = null just ignore it)
            //insert all that have type = null and DeleteMe == false
            for(int k = 0; k < allVersions.size(); k++) {
                CandidateForDeletion candidate = allVersions.get(k);
                if(candidate.getType() == null) {
                    //these are the versions from the BatchInsertIterator
                    if(!candidate.getDeleteMe()) {
                        //insert
                        tempFile.insertMap(candidate.getMap());
                    }
                } else {
                    //these are the versions from the other storages in this BigT
                    if(candidate.getDeleteMe()) {
                        //delete
                        if(candidate.getType() == StorageType.TYPE_0) {
                            VMapfile storageThatNeedsADeletion = (VMapfile)storageTypes.get(candidate.getType());
                            storageThatNeedsADeletion.deleteMap(candidate.getMID());
                        } else {
                            SmallMapFile storageThatNeedsADeletion = (SmallMapFile) storageTypes.get(candidate.getType());
                            storageThatNeedsADeletion.deleteMap(candidate.getMID());
                        }

                        System.out.print("deleting from type " + candidate.getType() + ": ");
                        candidate.getMap().print();

                    }
                }
            }

            //empty the latest 3 array for next pass
            latestFromSort[0] = null;
            latestFromSort[1] = null;
            latestFromSort[2] = null;
        }
        itr.closestream();

        //AT THIS POINT we have removed all necessary old versions from other storage types
        //and have a temporary VMapFile that contains everything from
        //the batchinsertion
        //lets delete the old file
        storageTypes.get(type).deleteFile();
        storageTypes.remove(type);

        //lets recreate the file and insert into it
        createFileOfType(type);
        Iterator tempIterator = new StreamIterator(((VMapfile)tempFile).openStream());

        //if its type 5 - we have to resort to get in row,value order
        if(type == StorageType.TYPE_4) {
            tempIterator = new Sort(attrType, (short) 4, attrSize, tempIterator, new int[]{0,3}, new TupleOrder(TupleOrder.Ascending), MAXROWLABELSIZE, (int)(numbuf * 0.4));
        }

        //insert everything
        Map temptempMap = tempIterator.get_next();
        while(temptempMap != null) {
            if(type == StorageType.TYPE_0) {
                ((VMapfile)storageTypes.get(type)).insertMap(temptempMap);
            } else {
                ((SmallMapFile)storageTypes.get(type)).insertMap(temptempMap);
            }
            temptempMap.print();

            temptempMap = tempIterator.get_next();
        }
        tempIterator.close();

        //rebuild the indexes
        for(StorageType t : StorageType.values()) {
            if (t != StorageType.TYPE_0) {
                reIndex(t);
            }
        }

        //cleanup
        //delete temp VMapfile
        tempFile.deleteFile();
    }

    //this one should check for versions
    public MID mapInsert(Map map, StorageType type) throws HFDiskMgrException, InvalidTupleSizeException, Exception, IOException, InvalidSlotNumberException, HFBufMgrException, SpaceNotAvailableException, ConstructPageException, GetFileEntryException, AddFileEntryException, FileScanException, InvalidRelation, InvalidMapSizeException {

        //put a list of all the versions - we should only find 3
        ArrayList<CandidateForDeletion> allVersions = new ArrayList<CandidateForDeletion>();

        //search for other versions of the map and add them to list
        for(StorageType t : StorageType.values()) {
            CondExpr[] filter = FilterParser.parseCombine(String.join("##", map.getRowLabel(), map.getColumnLabel(), "*"));
            DatafileIterator fileToBeSearched;
            if(t==StorageType.TYPE_0) {
                fileToBeSearched = ((VMapfile)storageTypes.get(t)).openStream();
            } else {
                fileToBeSearched = ((SmallMapFile)storageTypes.get(t)).openStream();
            }
            DatafileIterator searchIterator = new FileStreamIterator(fileToBeSearched, filter);

            MID mid = new MID();
            Map tempMapFromSearch = searchIterator.getNext(mid);
            while(tempMapFromSearch != null) {
                allVersions.add(new CandidateForDeletion(tempMapFromSearch, mid, t));

                tempMapFromSearch = searchIterator.getNext(mid);
            }

            //cleanup
            fileToBeSearched.closestream();
            searchIterator.closestream();
        }

        //add in the new map to the allVersions array incase it has an old timestamp
        allVersions.add(new CandidateForDeletion(map, null, null));

        //AT THIS POINT we have the list of all versions of the map
        //if there are more than 3 total versions - we will either have to delete some
        //or ignore some from the input map
        //loop through 3 times, selecting the highest timestamp values and set those not for deletion
        if(allVersions.size() > 3) {
            for (int k = 0; k < 3; k++) {
                int maxTimeStamp = -1;
                int indexOfMaxTimeStamp = -1;

                for(int j = 0; j < allVersions.size(); j++){
                    if(allVersions.get(j).getMap().getTimeStamp() > maxTimeStamp && allVersions.get(j).getDeleteMe()) {
                        maxTimeStamp = allVersions.get(j).getMap().getTimeStamp();
                        indexOfMaxTimeStamp = j;
                    }
                }

                //mark this highest time stamp(that hasn't already been marked) to not be deleted
                allVersions.get(indexOfMaxTimeStamp).setDeleteMe(false);
            }
        } else {
            //none are going to be deleted
            for(int k = 0; k < allVersions.size(); k++) {
                allVersions.get(k).setDeleteMe(false);
            }
        }

        //AT THIS POINT we have the same array of candidates but now 3 are set to not be deleted
        //loop through and delete any that have DeleteMe == true (or if type = null just ignore it and return)
        //insert all that have type = null and DeleteMe == false
        MID mid = null;
        for(int k = 0; k < allVersions.size(); k++) {
            CandidateForDeletion candidate = allVersions.get(k);
            if(candidate.getType() == null) {
                //this is the version from the input map
                if(!candidate.getDeleteMe()) {
                    //insert
                    if(type == StorageType.TYPE_0) {
                        mid = ((VMapfile)storageTypes.get(type)).insertMap(candidate.getMap());
                    } else {
                        mid = ((SmallMapFile)storageTypes.get(type)).insertMap(candidate.getMap());
                        //TODO THIS IS WHERE WE WOULD NEED TO HANDLE STUFF FOR THE INDEX
                        //TODO I LEAVE THIS FOR SOMEONE ELSE TO DO




                    }
                } else {
                    return null;
                }
            } else {
                //these are the versions from the other storages in this BigT
                if(candidate.getDeleteMe()) {
                    //delete
                    if(candidate.getType() == StorageType.TYPE_0) {
                        VMapfile storageThatNeedsADeletion = (VMapfile)storageTypes.get(candidate.getType());
                        storageThatNeedsADeletion.deleteMap(candidate.getMID());
                    } else {
                        SmallMapFile storageThatNeedsADeletion = (SmallMapFile) storageTypes.get(candidate.getType());
                        BTreeFile indexThatNeedsADeletion = indexTypes.get(candidate.getType());
                        storageThatNeedsADeletion.deleteMap(candidate.getMID());
                        KeyClass key;
                        switch (candidate.getType()){
                            case TYPE_1: key = new StringKey(candidate.getMap().getRowLabel());
                                break;
                            case TYPE_2: key = new StringKey(candidate.getMap().getColumnLabel());
                                break;
                            case TYPE_3: key = new StringKey(String.join(DELIMITER, candidate.getMap().getColumnLabel(), candidate.getMap().getRowLabel()));
                                break;
                            case TYPE_4: key = new StringKey(String.join(DELIMITER, candidate.getMap().getRowLabel(), candidate.getMap().getValue()));
                                break;
                            default:
                                throw new HFException(null, "Invalid Storage Type!");
                        }
                        indexThatNeedsADeletion.Delete(key, new RID(candidate.getMID()));
                    }

                    System.out.print("deleting from type " + type + ": ");
                    candidate.getMap().print();
                }
            }
        }

        return mid;
    }

    public Iterator query(int orderType, String rowFilter, String columnFilter, String valueFilter, int num_pages) throws FileScanException, IOException, InvalidRelation, SortException, HFBufMgrException {
        // Could use indexes, or filescan with filterstream or whatever
        if(orderType == 6){
            MultiTypeFileStream ms = new MultiTypeFileStream(this, FilterParser.parseCombine(String.join("##", rowFilter, columnFilter, valueFilter)));
            return ms;
        }
        SortTypeMap.init();
        MultiTypeFileStream ms = new MultiTypeFileStream(this, FilterParser.parseCombine(String.join("##", rowFilter, columnFilter, valueFilter)));
        return new Sort(attrType, (short) 4, attrSize, ms, SortTypeMap.returnSortOrderArray(orderType - 1), new TupleOrder(TupleOrder.Ascending), MAXROWLABELSIZE, (int)(num_pages * 0.6));
    }

    public Integer getRowCount() throws Exception {
        // Unique rows
        MultiTypeFileStream ms = new MultiTypeFileStream(this,null);
        Sort st = new Sort(attrType, (short) 4, attrSize, ms, new int[]{0}, new TupleOrder(TupleOrder.Ascending), MAXROWLABELSIZE, 134);
        Map m = st.get_next();
        String s = "";
        int c = 0;
        if (m != null) {
            c = 1;
            s = m.getRowLabel();
            while ((m = st.get_next()) != null) {
                if (!s.equalsIgnoreCase(m.getRowLabel()))
                    c++;
                s = m.getRowLabel();
            }
        }
        st.close();
        ms.close();
        return c;
    }

    public Integer getColumnCount() throws Exception {
        // Unique columns
        MultiTypeFileStream ms = new MultiTypeFileStream(this,null);
        Sort st = new Sort(attrType, (short) 4, attrSize, ms, new int[]{1}, new TupleOrder(TupleOrder.Ascending), MAXCOLUMNLABELSIZE, 134);
        Map m = st.get_next();
        String s = "";
        int c = 0;
        if (m != null) {
            c = 1;
            s = m.getColumnLabel();
            while ((m = st.get_next()) != null) {
                if (!s.equalsIgnoreCase(m.getColumnLabel()))
                    c++;
                s = m.getColumnLabel();
            }
        }
        st.close();
        ms.close();
        return c;
    }

    public Integer getMapCount() throws HFBufMgrException, IOException, HFDiskMgrException, InvalidSlotNumberException, InvalidTupleSizeException {
        // Does this include all versions?
        int total_count = 0;
        for (StorageType type : StorageType.values()) {
            total_count += storageTypes.get(type).getMapCnt();
        }
        return total_count;
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

    //This is for rowJoin and rowSort - they only insert into a type 1 storage
    public void insertMap(Map map) throws HFDiskMgrException, InvalidTupleSizeException, HFException, IOException, InvalidSlotNumberException, SpaceNotAvailableException, HFBufMgrException {
        ((VMapfile)storageTypes.get(StorageType.TYPE_0)).insertMap(map);
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

    // method to reindex a specific index file type.
    public void reIndex(StorageType type) throws DeleteFileEntryException,
            IteratorException,
            PinPageException,
            IOException,
            ConstructPageException,
            FreePageException,
            UnpinPageException,
            HFBufMgrException,
            PageNotReadException,
            PagePinnedException,
            HashOperationException,
            ReplacerException,
            BufferPoolExceededException,
            BufMgrException,
            InvalidSlotNumberException,
            InvalidTupleSizeException,
            InvalidFrameNumberException,
            PageUnpinnedException,
            HashEntryNotFoundException,
            HFException,
            GetFileEntryException,
            AddFileEntryException,
            NodeNotMatchException,
            LeafDeleteException,
            LeafInsertRecException,
            IndexSearchException,
            InsertException,
            ConvertException,
            DeleteRecException,
            KeyNotMatchException,
            KeyTooLongException,
            IndexInsertRecException {
        BTreeFile bf = indexTypes.get(type);
        SmallMapFile hf = (SmallMapFile) storageTypes.get(type);
        storage.Stream s = hf.openStream();
        Map insMap;
        MID tmpM = new MID();
        bf.close();
        bf.destroyFile();
        switch (type){
            case TYPE_1: indexTypes.put(type, new BTreeFile(generateIndexName(type), AttrType.attrString, GlobalConst.MAXROWLABELSIZE, 1));
                break;
            case TYPE_2: indexTypes.put(type, new BTreeFile(generateIndexName(type), AttrType.attrString, GlobalConst.MAXCOLUMNLABELSIZE, 1));
                break;
            case TYPE_3: indexTypes.put(type, new BTreeFile(generateIndexName(type), AttrType.attrString, GlobalConst.MAXCOLUMNLABELSIZE + GlobalConst.MAXROWLABELSIZE + 1, 1));
                break;
            case TYPE_4: indexTypes.put(type, new BTreeFile(generateIndexName(type), AttrType.attrString, GlobalConst.MAXROWLABELSIZE + GlobalConst.MAXVALUESIZE + 1, 1));
                break;
            default:
                throw new HFException(null, "Invalid Storage Type!");
        }
        bf = indexTypes.get(type);
        while((insMap = s.getNext(tmpM))!=null){
            switch (type){
                case TYPE_1: bf.insert(new StringKey(insMap.getRowLabel()), new RID(tmpM));
                    break;
                case TYPE_2: bf.insert(new StringKey(insMap.getColumnLabel()), new RID(tmpM));
                    break;
                case TYPE_3: bf.insert(new StringKey(String.join(DELIMITER, insMap.getColumnLabel(), insMap.getRowLabel())), new RID(tmpM));
                    break;
                case TYPE_4: bf.insert(new StringKey(String.join(DELIMITER, insMap.getRowLabel(), insMap.getValue())), new RID(tmpM));
                    break;
                default:
                    throw new HFException(null, "Invalid Storage Type!");
            }
        }
        s.closestream();
    }


    private int[] getSortOrder(StorageType type) {
        int[] sortOrder;
        switch(type) {
            case TYPE_0:
            case TYPE_1:
                sortOrder = new int[]{0,1,2};
                break;
            case TYPE_2:
            case TYPE_3:
            case TYPE_4:
                sortOrder = new int[]{1,0,2};
                break;
            default:
                sortOrder = null;
                break;
        }
        return sortOrder;
    }

    public void delete_all_files() throws HFDiskMgrException, InvalidTupleSizeException, IOException, InvalidSlotNumberException, FileAlreadyDeletedException, HFBufMgrException, DeleteFileEntryException, IteratorException, PinPageException, ConstructPageException, FreePageException, UnpinPageException, PageUnpinnedException, InvalidFrameNumberException, HashEntryNotFoundException, ReplacerException {
        for (StorageType type : StorageType.values()) {
            storageTypes.get(type).deleteFile();
            if(type != StorageType.TYPE_0) {
                indexTypes.get(type).close();
                indexTypes.get(type).destroyFile();
            }
        }
    }
}
