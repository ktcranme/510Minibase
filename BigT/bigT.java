package BigT;

import btree.*;
import bufmgr.HashEntryNotFoundException;
import bufmgr.InvalidFrameNumberException;
import bufmgr.PageUnpinnedException;
import bufmgr.ReplacerException;
import driver.FilterParser;
import global.*;
import heap.*;
import index.IndexException;
import index.UnknownIndexTypeException;
import iterator.CondExpr;
import iterator.FileScanException;
import iterator.InvalidRelation;

import java.io.IOException;

public class bigT implements GlobalConst {
    public static final String INDEXFILENAMEPREFIX = "bigTInd";
    public static final String TSINDEXFILENAMEPREFIX = "bigTIndTS";
    public static final String DELIMITER = ",";
    private int type;
    private String name;
    private Mapfile hf;
    private BTreeFile btf;
    private BTreeFile btfTS;
    AttrType[] attrType = {new AttrType(AttrType.attrString), new AttrType(AttrType.attrString), new AttrType(AttrType.attrInteger), new AttrType(AttrType.attrString)};
    short[] attrSize = {MAXROWLABELSIZE, MAXCOLUMNLABELSIZE, MAXVALUESIZE};

    public Mapfile getHf() {
        return hf;
    }

    public void setHf(Mapfile hf) {
        this.hf = hf;
    }

    public BTreeFile getBtf() {
        return btf;
    }

    public void setBtf(BTreeFile btf) {
        this.btf = btf;
    }

    public BTreeFile getBtfTS() {
        return btfTS;
    }

    public void setBtfTS(BTreeFile btfTS) {
        this.btfTS = btfTS;
    }

    public int getType() {
        return type;
    }

    public void setType(int type) {
        this.type = type;
    }

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public bigT(String name, int type) throws IOException,
            HFDiskMgrException,
            HFBufMgrException,
            HFException,
            ConstructPageException,
            GetFileEntryException,
            AddFileEntryException {
        this.name = name;
        this.type = type;
        this.hf = new Mapfile(name);
        switch (type) {
            case 1:
                this.btf = null;// no index
                break;
            case 2:
                this.btf = new BTreeFile(INDEXFILENAMEPREFIX.concat(name), AttrType.attrString, GlobalConst.MAXROWLABELSIZE, 1);
                break;
            case 3:
                this.btf = new BTreeFile(INDEXFILENAMEPREFIX.concat(name), AttrType.attrString, GlobalConst.MAXCOLUMNLABELSIZE, 1);
                break;
            case 4: //one btree to index column label and row label (combined key) and one btree to index timestamps
                this.btf = new BTreeFile(INDEXFILENAMEPREFIX.concat(name), AttrType.attrString, GlobalConst.MAXCOLUMNLABELSIZE + GlobalConst.MAXROWLABELSIZE + 1, 1);
                this.btfTS = new BTreeFile(TSINDEXFILENAMEPREFIX.concat(name), AttrType.attrInteger, 4, 1);
                break;
            case 5: //one btree to index row label and value (combined key) and one btree to index timestamps
                this.btf = new BTreeFile(INDEXFILENAMEPREFIX.concat(name), AttrType.attrString, GlobalConst.MAXROWLABELSIZE + GlobalConst.MAXVALUESIZE + 1, 1);
                this.btfTS = new BTreeFile(TSINDEXFILENAMEPREFIX.concat(name), AttrType.attrInteger, 4, 1);
                break;
        }
    }

    public void deleteBigt() throws IOException,
            HFDiskMgrException,
            FileAlreadyDeletedException,
            HFBufMgrException,
            InvalidSlotNumberException,
            InvalidTupleSizeException {
        hf.deleteFile();
    }

    public int getMapCnt() throws HFBufMgrException,
            IOException,
            HFDiskMgrException,
            InvalidSlotNumberException,
            InvalidTupleSizeException {
        return hf.getMapCnt();
    }

    public int getColumnCnt() throws Exception {
        BTFileScan bts;
        KeyDataEntry ky;
        String s = "";
        int c = 0;
        switch (type) {
            case 3:
                bts = getBtf().new_scan(null, null);
                ky = bts.get_next();
                if (ky != null) {
                    c = 1;
                    s = ky.key.toString();
                    while ((ky = bts.get_next()) != null) {
                        if (!s.equalsIgnoreCase(ky.key.toString()))
                            c++;
                        s = ky.key.toString();
                    }
                }
                bts.DestroyBTreeFileScan();
                break;
            case 4:
                bts = getBtf().new_scan(null, null);
                ky = bts.get_next();
                if (ky != null) {
                    c = 1;
                    s = ky.key.toString().split(DELIMITER)[0];
                    while ((ky = bts.get_next()) != null) {
                        if (!s.equalsIgnoreCase(ky.key.toString().split(DELIMITER)[0]))
                            c++;
                        s = ky.key.toString().split(DELIMITER)[0];
                    }
                }
                bts.DestroyBTreeFileScan();
                break;
            default:
                FileStream fs = new FileStream(hf, null);
                Sort st = new Sort(attrType, (short) 4, attrSize, fs, new int[]{1}, new TupleOrder(TupleOrder.Ascending), MAXCOLUMNLABELSIZE, 134);
                Map m = st.get_next();
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
                fs.close();
                break;
        }
        return c;
    }

    public int getRowCnt() throws Exception {
        BTFileScan bts;
        KeyDataEntry ky;
        String s = "";
        int c = 0;
        switch (type) {
            case 2:
                bts = getBtf().new_scan(null, null);
                ky = bts.get_next();
                if (ky != null) {
                    c = 1;
                    s = ky.key.toString();
                    while ((ky = bts.get_next()) != null) {
                        if (!s.equalsIgnoreCase(ky.key.toString()))
                            c++;
                        s = ky.key.toString();
                    }
                }
                bts.DestroyBTreeFileScan();
                break;
            case 5:
                bts = getBtf().new_scan(null, null);
                ky = bts.get_next();
                if (ky != null) {
                    c = 1;
                    s = ky.key.toString().split(DELIMITER)[0];
                    while ((ky = bts.get_next()) != null) {
                        if (!s.equalsIgnoreCase(ky.key.toString().split(DELIMITER)[0]))
                            c++;
                        s = ky.key.toString().split(DELIMITER)[0];
                    }
                }
                bts.DestroyBTreeFileScan();
                break;
            default:
                FileStream fs = new FileStream(hf, null);
                Sort st = new Sort(attrType, (short) 4, attrSize, fs, new int[]{0}, new TupleOrder(TupleOrder.Ascending), MAXROWLABELSIZE, 134);
                Map m = st.get_next();
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
                fs.close();
                break;
        }
        return c;
    }

    public MID insertMapBulk(byte[] mapPtr, BTreeFile tempFile) throws Exception {
        //need to handle versioning to remove 4th version
        Map tempMap, delMap;
        MID mid, tmpmid, tm = null;
        delMap = new Map();
        switch (type) {
            case 1: // no index update required
                tempMap = new Map(mapPtr, 0);
                tmpmid = crIndexMapFind(tempMap,tempFile);
                if (tmpmid != null) {
                    tm = hf.updateMap(tmpmid, tempMap, delMap);
                    if (tm == null)
                        return null;
                    if (!tm.isReused) {
                        tempFile.insert(new StringKey(String.join(DELIMITER, tempMap.getColumnLabel(), tempMap.getRowLabel())), new RID(tm));
                    }
                } else {
                    tm = hf.insertMap(tempMap);
                    tempFile.insert(new StringKey(String.join(DELIMITER, tempMap.getColumnLabel(), tempMap.getRowLabel())), new RID(tm));
                }
                break;
            case 2:
                tempMap = new Map(mapPtr, 0);
                tmpmid = crIndexMapFind(tempMap,tempFile);
                if (tmpmid != null) {
                    tm = hf.updateMap(tmpmid, tempMap, delMap);
                    if (tm == null)
                        return null;
                    if (!tm.isReused) {
                        tempFile.insert(new StringKey(String.join(DELIMITER, tempMap.getColumnLabel(), tempMap.getRowLabel())), new RID(tm));
                        btf.insert(new StringKey(tempMap.getRowLabel()), new RID(tm));
                    }
                } else {
                    tm = hf.insertMap(tempMap);
                    tempFile.insert(new StringKey(String.join(DELIMITER, tempMap.getColumnLabel(), tempMap.getRowLabel())), new RID(tm));
                    btf.insert(new StringKey(tempMap.getRowLabel()), new RID(tm));
                }
                break;
            case 3:
                tempMap = new Map(mapPtr, 0);
                tmpmid = crIndexMapFind(tempMap,tempFile);
                if (tmpmid != null) {
                    tm = hf.updateMap(tmpmid, tempMap, delMap);
                    if (tm == null)
                        return null;
                    if (!tm.isReused) {
                        tempFile.insert(new StringKey(String.join(DELIMITER, tempMap.getColumnLabel(), tempMap.getRowLabel())), new RID(tm));
                        btf.insert(new StringKey(tempMap.getColumnLabel()), new RID(tm));
                    }
                } else {
                    tm = hf.insertMap(tempMap);
                    tempFile.insert(new StringKey(String.join(DELIMITER, tempMap.getColumnLabel(), tempMap.getRowLabel())), new RID(tm));
                    btf.insert(new StringKey(tempMap.getColumnLabel()), new RID(tm));
                }
                break;
            case 4: //one btree to index column label and row label (combined key) and one btree to index timestamps
                insertMap(mapPtr);
            case 5: //one btree to index row label and value (combined key) and one btree to index timestamps
                tempMap = new Map(mapPtr, 0);
                tmpmid = crIndexMapFind(tempMap,tempFile);
                if (tmpmid != null) {
                    tm = hf.updateMap(tmpmid, tempMap, delMap);
                    if (tm == null)
                        return null;
                    if (!tm.isReused) {
                        btf.insert(new StringKey(String.join(DELIMITER, tempMap.getRowLabel(), tempMap.getValue())), new RID(tm));
                        tempFile.insert(new StringKey(String.join(DELIMITER, tempMap.getColumnLabel(), tempMap.getRowLabel())), new RID(tm));
                        btfTS.insert(new IntegerKey(tempMap.getTimeStamp()), new RID(tm));
                    } else {
                        btfTS.Delete(new IntegerKey(delMap.getTimeStamp()), new RID(tm));
                        btfTS.insert(new IntegerKey(tempMap.getTimeStamp()), new RID(tm));
                    }
                } else {
                    tm = hf.insertMap(tempMap);
                    btf.insert(new StringKey(String.join(DELIMITER, tempMap.getRowLabel(), tempMap.getValue())), new RID(tm));
                    tempFile.insert(new StringKey(String.join(DELIMITER, tempMap.getColumnLabel(), tempMap.getRowLabel())), new RID(tm));
                    btfTS.insert(new IntegerKey(tempMap.getTimeStamp()), new RID(tm));
                }
                break;
        }
        return tm;
    }

    public MID insertMap(byte[] mapPtr) throws Exception {
        //need to handle versioning to remove 4th version
        Map tempMap, delMap;
        MID mid, tmpmid, tm = null;
        delMap = new Map();
        switch (type) {
            case 1: // no index update required
                tempMap = new Map(mapPtr, 0);
                tmpmid = naiveMapFind(tempMap);
                if (tmpmid != null) {
                    tm = hf.updateMap(tmpmid, tempMap);
                } else {
                    tm = hf.insertMap(tempMap);
                }
                break;
            case 2:
                tempMap = new Map(mapPtr, 0);
                tmpmid = naiveMapFind(tempMap);
                if (tmpmid != null) {
                    tm = hf.updateMap(tmpmid, tempMap);
                    if (tm != null && !tm.isReused) {
                        btf.insert(new StringKey(tempMap.getRowLabel()), new RID(tm));
                    }
                } else {
                    tm = hf.insertMap(tempMap);
                    btf.insert(new StringKey(tempMap.getRowLabel()), new RID(tm));
                }
                break;
            case 3:
                tempMap = new Map(mapPtr, 0);
                tmpmid = naiveMapFind(tempMap);
                if (tmpmid != null) {
                    tm = hf.updateMap(tmpmid, tempMap);
                    if (tm != null && !tm.isReused) {
                        btf.insert(new StringKey(tempMap.getColumnLabel()), new RID(tm));
                    }
                } else {
                    tm = hf.insertMap(tempMap);
                    btf.insert(new StringKey(tempMap.getColumnLabel()), new RID(tm));
                }
                break;
            case 4: //one btree to index column label and row label (combined key) and one btree to index timestamps
                tempMap = new Map(mapPtr, 0);
                tmpmid = crIndexMapFind(tempMap,btf);
                if (tmpmid != null) {
                    tm = hf.updateMap(tmpmid, tempMap, delMap);
                    if (tm == null)
                        return null;
                    if (!tm.isReused) {
                        btf.insert(new StringKey(String.join(DELIMITER, tempMap.getColumnLabel(), tempMap.getRowLabel())), new RID(tm));
                        btfTS.insert(new IntegerKey(tempMap.getTimeStamp()), new RID(tm));
                    } else {
                        btfTS.Delete(new IntegerKey(delMap.getTimeStamp()), new RID(tm));
                        btfTS.insert(new IntegerKey(tempMap.getTimeStamp()), new RID(tm));
                    }
                } else {
                    tm = hf.insertMap(tempMap);
                    btf.insert(new StringKey(String.join(DELIMITER, tempMap.getColumnLabel(), tempMap.getRowLabel())), new RID(tm));
                    btfTS.insert(new IntegerKey(tempMap.getTimeStamp()), new RID(tm));
                }
                break;
            case 5: //one btree to index row label and value (combined key) and one btree to index timestamps
                tempMap = new Map(mapPtr, 0);
                tmpmid = naiveMapFind(tempMap);
                if (tmpmid != null) {
                    tm = hf.updateMap(tmpmid, tempMap, delMap);
                    if (tm == null)
                        return null;
                    if (!tm.isReused) {
                        btf.insert(new StringKey(String.join(DELIMITER, tempMap.getRowLabel(), tempMap.getValue())), new RID(tm));
                        btfTS.insert(new IntegerKey(tempMap.getTimeStamp()), new RID(tm));
                    } else {
                        btfTS.Delete(new IntegerKey(delMap.getTimeStamp()), new RID(tm));
                        btfTS.insert(new IntegerKey(tempMap.getTimeStamp()), new RID(tm));
                    }
                } else {
                    tm = hf.insertMap(tempMap);
                    btf.insert(new StringKey(String.join(DELIMITER, tempMap.getRowLabel(), tempMap.getValue())), new RID(tm));
                    btfTS.insert(new IntegerKey(tempMap.getTimeStamp()), new RID(tm));
                }
                break;
        }
        return tm;
    }

    MID naiveMapFind(Map findMap) throws HFBufMgrException,
            IOException,
            InvalidMapSizeException,
            InvalidSlotNumberException,
            InvalidTupleSizeException {
        Stream s = new Stream(hf);
        MID mid = new MID();
        Map m;
        while ((m = s.getNext(mid)) != null) {
            if (m.getRowLabel().equalsIgnoreCase(findMap.getRowLabel()) && m.getColumnLabel().equalsIgnoreCase(findMap.getColumnLabel())) {
                s.closestream();
                return mid;
            }
        }
        s.closestream();
        return null;
    }

    public Iterator openStream(int orderType, String rowFilter, String columnFilter, String valueFilter) throws Exception {
        SortTypeMap.init();
        Iterator it = null;
        Iterator tmp = null;
        switch (type) {
            case 1:
                tmp = filterVal("scan", rowFilter, columnFilter, valueFilter);
                it = new Sort(attrType, (short) 4, attrSize, tmp, SortTypeMap.returnSortOrderArray(orderType - 1), new TupleOrder(TupleOrder.Ascending), MAXROWLABELSIZE, 134);
                break;
            case 2:
                if(rowFilter.charAt(0) != '*' && rowFilter.charAt(0) != '[')
                {
                    tmp = filterVal("i2REqual", rowFilter, columnFilter, valueFilter);
                    it = new Sort(attrType, (short) 4, attrSize, tmp, SortTypeMap.returnSortOrderArray(orderType - 1), new TupleOrder(TupleOrder.Ascending), MAXROWLABELSIZE, GlobalConst.NUMBUF);
                    break;
                }
                else
                {
                    tmp = filterVal("scan", rowFilter, columnFilter, valueFilter);
                    it = new Sort(attrType, (short) 4, attrSize, tmp, SortTypeMap.returnSortOrderArray(orderType - 1), new TupleOrder(TupleOrder.Ascending), MAXROWLABELSIZE, 134);
                    break;
                }
            case 3:
                if(columnFilter.charAt(0) != '*' && columnFilter.charAt(0) != '[')
                {
                    tmp = filterVal("i3CEqual", rowFilter, columnFilter, valueFilter);
                    it = new Sort(attrType, (short) 4, attrSize, tmp, SortTypeMap.returnSortOrderArray(orderType - 1), new TupleOrder(TupleOrder.Ascending), MAXROWLABELSIZE, GlobalConst.NUMBUF);
                    break;
                }
                else
                {
                    tmp = filterVal("scan", rowFilter, columnFilter, valueFilter);
                    it = new Sort(attrType, (short) 4, attrSize, tmp, SortTypeMap.returnSortOrderArray(orderType - 1), new TupleOrder(TupleOrder.Ascending), MAXROWLABELSIZE, 134);
                    break;
                }
            case 4:
                if (orderType == 5) {
                    it = filterVal("i4o5", rowFilter, columnFilter, valueFilter);
                } else {
                    tmp = filterVal("scan", rowFilter, columnFilter, valueFilter);
                    it = new Sort(attrType, (short) 4, attrSize, tmp, SortTypeMap.returnSortOrderArray(orderType - 1), new TupleOrder(TupleOrder.Ascending), MAXROWLABELSIZE, 134);
                }
                break;
            case 5:
                if (orderType == 5) {
                    it = filterVal("i5o5", rowFilter, columnFilter, valueFilter);
                } else {
                    tmp = filterVal("scan", rowFilter, columnFilter, valueFilter);
                    it = new Sort(attrType, (short) 4, attrSize, tmp, SortTypeMap.returnSortOrderArray(orderType - 1), new TupleOrder(TupleOrder.Ascending), MAXROWLABELSIZE, 134);
                }
                break;
        }
        return it;
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

    Iterator filterVal(String filterSec, String rowFilter, String columnFilter, String valueFilter) throws FileScanException, IOException, InvalidRelation, IndexException, UnknownIndexTypeException {
        Iterator it = null;
        CondExpr[] filter;
        switch (filterSec) {
            case "i2REqual":
                //filter = FilterParser.parseCombine(String.join("##", rowFilter, columnFilter, valueFilter));
                //CondExpr[] rowEqualityCondExpr = FilterParser.parseSingleIndexEquality(rowFilter,1,AttrType.attrString);
                //it = new IndexScan(new IndexType(IndexType.B_Index), name, INDEXFILENAMEPREFIX + name, rowEqualityCondExpr, filter, false);
                StringKey rowFilterKey = new StringKey(rowFilter);
                it = getBtf().new_scan(rowFilterKey,rowFilterKey);
                break;
            case "i3CEqual":
                filter = FilterParser.parseCombine(String.join("##", rowFilter, columnFilter, valueFilter));
                CondExpr[] columnEqualityCondExpr = FilterParser.parseSingleIndexEquality(columnFilter,2,AttrType.attrString);
                it = new IndexScan(new IndexType(IndexType.B_Index), name, INDEXFILENAMEPREFIX + name, columnEqualityCondExpr, filter, false);
                break;
            case "i4o5":
            case "i5o5":
                filter = FilterParser.parseCombine(String.join("##", rowFilter, columnFilter, valueFilter));
                it = new IndexScan(new IndexType(IndexType.B_Index), name, TSINDEXFILENAMEPREFIX + name, null, filter, false);
                break;
            case "scan":
                filter = FilterParser.parseCombine(String.join("##", rowFilter, columnFilter, valueFilter));
                it = new FileStream(hf, filter);
        }
        return it;
    }
}