package BigT;

import driver.FilterParser;
import global.AttrType;
import global.GlobalConst;
import global.TupleOrder;
import storage.BigT;

public class RowSort implements GlobalConst {
    static AttrType[] attrType = {new AttrType(AttrType.attrString), new AttrType(AttrType.attrString), new AttrType(AttrType.attrInteger), new AttrType(AttrType.attrString)};
    static short[] attrSize = {MAXROWLABELSIZE, MAXCOLUMNLABELSIZE, MAXVALUESIZE};
    public static BigT rowSort(BigT b, String out ,String columnFilter, int num_pages) throws Exception {
        MultiTypeFileStream fs = null, fsRow = null;
        LatestVersion lv = null;
        Sort s = null;
        BigT outB = null, tempFile = null;
        try {
            fs = new MultiTypeFileStream(b, FilterParser.parseSingle(columnFilter, 2, AttrType.attrString));
            lv = new LatestVersion(fs, num_pages);
            s = new Sort(attrType, (short) 4, attrSize, lv, new int[]{3}, new TupleOrder(TupleOrder.Ascending), MAXROWLABELSIZE, (int) (num_pages * 0.4));
            Map tempMap, tempWriteMap;
            outB = new BigT(out);
            while ((tempMap = s.get_next()) != null) {
                fsRow = new MultiTypeFileStream(b, FilterParser.parseSingle(tempMap.getRowLabel(), 1, AttrType.attrString));
                while ((tempWriteMap = fsRow.get_next()) != null) {
                    outB.insertMap(tempWriteMap);
                }
                fsRow.close();
            }
            s.close();
            lv.close();
            fs.close();
            tempFile = createRemFile(b, columnFilter, num_pages);
            fs = new MultiTypeFileStream(tempFile, null);
            while ((tempWriteMap = fs.get_next()) != null) {
                outB.insertMap(tempWriteMap);
            }
            fs.close();
            if(tempFile!=null) {
                tempFile.delete_all_files();
            }
            return outB;
        } catch(Exception e){
            e.printStackTrace();
            if(fs!=null){ fs.close(); }
            if(fsRow!=null){ fsRow.close(); }
            if(lv!=null){ lv.close(); }
            if(s!=null){ s.close(); }
            if(outB!=null){ outB.close(); }
            if(tempFile!=null){ tempFile.delete_all_files(); tempFile.close(); }
            return null;
        }
    }

    public static BigT createRemFile(BigT b, String columnFilter, int num_pages) throws Exception {
        MultiTypeFileStream fs=null, fs1=null;
        BigT outB=null;
        Sort s = null;
        try {
            fs = new MultiTypeFileStream(b, null);
            s = new Sort(attrType, (short) 4, attrSize, fs, new int[]{0}, new TupleOrder(TupleOrder.Ascending), MAXROWLABELSIZE, (int) (num_pages * 0.4));
            outB = new BigT(b.getName().concat("remfile"));
            Map tempMap, prevMap = null, retMap, tMap;
            while ((tempMap = s.get_next()) != null) {
                if (prevMap == null) {
                    prevMap = new Map();
                    prevMap.mapCopy(tempMap);
                    if (tempMap.getColumnLabel().equalsIgnoreCase(columnFilter)) {
                        prevMap = new Map();
                        tMap = traverseNewRow(s, tempMap.getRowLabel());
                        if (tMap == null) {
                            break;
                        } else {
                            prevMap.mapCopy(tMap);
                        }
                    }
                    continue;
                }
                if (tempMap.getColumnLabel().equalsIgnoreCase(columnFilter)) {
                    prevMap = new Map();
                    tMap = traverseNewRow(s, tempMap.getRowLabel());
                    if (tMap == null) {
                        break;
                    } else {
                        prevMap.mapCopy(tMap);
                    }
                }
                if (!tempMap.getRowLabel().equalsIgnoreCase(prevMap.getRowLabel())) {
                    fs1 = new MultiTypeFileStream(b, FilterParser.parseSingle(prevMap.getRowLabel(), 1, AttrType.attrString));
                    while ((retMap = fs1.get_next()) != null) {
                        outB.insertMap(retMap);
                    }
                    prevMap = new Map();
                    prevMap.mapCopy(tempMap);
                    fs1.close();
                }
            }
            s.close();
            fs.close();
            return outB;
        } catch (Exception e) {
            e.printStackTrace();
            if(outB!=null){
                outB.delete_all_files(); outB.close(); }
            if(fs!=null){ fs.close(); }
            if(fs1!=null){ fs1.close(); }
            if(s!=null){ s.close(); }
            return null;
        }
    }

    public static Map traverseNewRow(Iterator it, String s) throws Exception {
        Map tMap;
        while((tMap = it.get_next())!=null){
            if(!tMap.getRowLabel().equalsIgnoreCase(s)){
                break;
            }
        }
        return tMap;
    }
}
