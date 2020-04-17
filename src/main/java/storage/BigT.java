package storage;

import java.io.IOException;
import java.sql.SQLOutput;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Random;

import BigT.VMapfile;
import BigT.Map;
import BigT.Iterator;
import BigT.DatafileIterator;
import BigT.Sort;
import BigT.FileStreamIterator;
import BigT.CandidateForDeletion;
import BigT.StreamIterator;
import btree.AddFileEntryException;
import btree.BTreeFile;
import btree.ConstructPageException;
import btree.GetFileEntryException;
import bufmgr.HashEntryNotFoundException;
import bufmgr.InvalidFrameNumberException;
import bufmgr.PageUnpinnedException;
import bufmgr.ReplacerException;
import driver.BatchInsertIterator;
import driver.FilterParser;
import global.*;
import heap.*;
import iterator.CondExpr;

import static global.GlobalConst.*;

public class BigT {
    String name;
    HashMap<StorageType, Heapfile> storageTypes;
    HashMap<StorageType, BTreeFile> indexTypes;
    Random rand = new Random();

    public BigT(String name) throws HFBufMgrException, HFException, IOException, ConstructPageException, AddFileEntryException, GetFileEntryException, HFDiskMgrException {
        this.name = name;
        storageTypes = new HashMap<>();
        indexTypes = new HashMap<>();

        for (StorageType type : StorageType.values()) {
            createStorageOfType(type);
        }
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

    public void batchInsert(Iterator inputFile, StorageType type, int numbuf) throws Exception {
        // Insert
        DatafileIterator dfItr;
        VMapfile tempFile = new VMapfile(generateBigTName(type) + "_temp");
        if (type == StorageType.TYPE_0) {
            VMapfile file = (VMapfile) storageTypes.get(type);
            dfItr = file.openStream();
        } else {
            SmallMapFile file = (SmallMapFile) storageTypes.get(type);
            dfItr = file.openStream();
        }


        Iterator i = new BatchInsertIterator(inputFile, dfItr);
        AttrType[] attrType = {new AttrType(AttrType.attrString), new AttrType(AttrType.attrString), new AttrType(AttrType.attrInteger), new AttrType(AttrType.attrString)};
        short[] attrSize = {MAXROWLABELSIZE, MAXCOLUMNLABELSIZE, MAXVALUESIZE};
        Iterator itr = new Sort(attrType, (short) 4, attrSize, i, getSortOrder(type), new TupleOrder(TupleOrder.Ascending), MAXROWLABELSIZE, (int)(numbuf * 0.8));
        //NOTE: this will sort either RCT or CRT in all cases - for case 5, a second sort is required to put it back in correct ordering.

        Map tempMap = itr.get_next();
        String currMapRC = "";
        Map[] latestFromSort = new Map[3];
        while(tempMap != null){
            currMapRC = tempMap.getRowLabel() + tempMap.getColumnLabel();

            int c = 0;
            while(tempMap != null && currMapRC.equals(tempMap.getRowLabel() + tempMap.getColumnLabel())) {
                latestFromSort[c%3] = new Map();
                latestFromSort[c%3].mapCopy(tempMap); //Note: this only works since we sort in timestamp order at the end
                c++;

                tempMap = itr.get_next();
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

                        System.out.print("deleting from type " + type + ": ");
                        candidate.getMap().print();
                    }
                }
            }



            //empty the latest 3 array for next pass
            latestFromSort[0] = null;
            latestFromSort[1] = null;
            latestFromSort[2] = null;
        }


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
            tempIterator = new Sort(attrType, (short) 4, attrSize, tempIterator, new int[]{0,3}, new TupleOrder(TupleOrder.Ascending), MAXROWLABELSIZE, (int)(numbuf * 0.8));
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

        //TODO REBUILD INDEX HERE

        //cleanup
        //delete temp VMapfile
        tempFile.deleteFile();
        //close all iterators used in this scope
        i.close();
        itr.close();
        tempIterator.close();
    }

    //this one should check for versions
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
}
