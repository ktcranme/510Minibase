package BigT;

import java.io.*;
import java.util.HashSet;
import java.util.Set;

import btree.*;
import bufmgr.HashEntryNotFoundException;
import bufmgr.InvalidFrameNumberException;
import bufmgr.PageUnpinnedException;
import bufmgr.ReplacerException;
import global.*;
import heap.*;
import iterator.*;

public class bigT implements GlobalConst {
    public static final String INDEXFILENAMEPREFIX = "bigTInd";
    public static final String TSINDEXFILENAMEPREFIX = "bigTIndTS";
    public static final String DELIMITER = "_";
    private int type;
    private String name;
    private Mapfile hf;
    private BTreeFile btf;
    private BTreeFile btfTS;

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
                Set<String> colNames =new HashSet<>();
                Stream scan = new Stream(hf);
                Map temp;
                MID rid = new MID();
                temp = scan.getNext(rid);
                while (temp != null) {
                    temp.mapCopy(temp);
                    colNames.add(temp.getColumnLabel());
                    temp = scan.getNext(rid);
                }
                c=colNames.size();
                break;
        }
        return c;
    }

    public int getRowCnt() throws IOException,
            KeyNotMatchException,
            IteratorException,
            ConstructPageException,
            PinPageException,
            UnpinPageException,
            ScanIteratorException, PageUnpinnedException, InvalidFrameNumberException, HashEntryNotFoundException, ReplacerException, HFBufMgrException, InvalidMapSizeException, InvalidSlotNumberException, InvalidTupleSizeException {
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
                Set<String> rowNames =new HashSet<>();
                Stream scan = new Stream(hf);
                Map temp;
                MID rid = new MID();
                temp = scan.getNext(rid);
                while (temp != null) {
                    temp.mapCopy(temp);
                    rowNames.add(temp.getRowLabel());
                    temp = scan.getNext(rid);
                }
                c=rowNames.size();
                break;
        }
        return c;
    }

    public RID insertMap(byte[] mapPtr) throws HFDiskMgrException,
            InvalidTupleSizeException,
            HFException,
            IOException,
            InvalidSlotNumberException,
            SpaceNotAvailableException,
            HFBufMgrException,
            UnpinPageException,
            DeleteRecException,
            ConvertException,
            PinPageException,
            LeafDeleteException,
            NodeNotMatchException,
            LeafInsertRecException,
            IndexInsertRecException,
            IndexSearchException,
            KeyTooLongException,
            KeyNotMatchException,
            ConstructPageException,
            IteratorException,
            InsertException {
        //need to handle versioning to remove 4th version
        Map tempMap = new Map(mapPtr, 0);
        MID mid = hf.insertMap(tempMap);
        RID rid = new RID(mid);
        switch (type) {
            case 1: // no index update required
                break;
            case 2:
                btf.insert(new StringKey(tempMap.getRowLabel()), rid);
                break;
            case 3:
                btf.insert(new StringKey(tempMap.getColumnLabel()), rid);
                break;
            case 4: //one btree to index column label and row label (combined key) and one btree to index timestamps
                btf.insert(new StringKey(String.join(DELIMITER, tempMap.getColumnLabel(), tempMap.getRowLabel())), rid);
                btfTS.insert(new IntegerKey(tempMap.getTimeStamp()), rid);
                break;
            case 5: //one btree to index row label and value (combined key) and one btree to index timestamps
                btf.insert(new StringKey(String.join(DELIMITER, tempMap.getRowLabel(), tempMap.getValue())), rid);
                btfTS.insert(new IntegerKey(tempMap.getTimeStamp()), rid);
                break;
        }
        return rid;
    }

    Stream openStream(int orderType, String rowFilter, String columnFilter, String value) throws InvalidMapSizeException,
            InvalidTupleSizeException,
            HFBufMgrException,
            IOException,
            InvalidSlotNumberException {
        Stream s;
        switch (orderType) {
            case 1:
            case 2:
            case 3:
            case 4:
            case 5:
            default:
                s = new Stream(hf);
                break;
        }
        return s;
    }
}