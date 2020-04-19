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
        MultiTypeFileStream fs = new MultiTypeFileStream(b, FilterParser.parseSingle(columnFilter,2, AttrType.attrString));
        LatestVersion lv = new LatestVersion(fs,num_pages);
        Sort s = new Sort(attrType, (short) 4, attrSize, lv, new int[]{3}, new TupleOrder(TupleOrder.Ascending), MAXROWLABELSIZE, 134);
        Map tempMap, tempWriteMap;
        BigT outB = new BigT(out);
        while ((tempMap = s.get_next())!=null){
            MultiTypeFileStream fsRow = new MultiTypeFileStream(b, FilterParser.parseSingle(tempMap.getRowLabel(),1, AttrType.attrString));
            //System.out.print("Main map : ");
            //tempMap.print();
            while ((tempWriteMap = fsRow.get_next())!=null){
                //tempWriteMap.print();
                outB.insertMap(tempWriteMap);
            }
            fsRow.close();
        }
        s.close();
        lv.close();
        fs.close();
        BigT tempFile = createRemFile(b,columnFilter,num_pages);
        fs = new MultiTypeFileStream(tempFile, null);
        while ((tempWriteMap = fs.get_next())!=null){
            //tempWriteMap.print();
            outB.insertMap(tempWriteMap);
        }
        fs.close();
        tempFile.delete_all_files();
        return outB;
    }

    public static BigT createRemFile(BigT b, String columnFilter, int num_pages) throws Exception {
        MultiTypeFileStream fs = new MultiTypeFileStream(b, null);
        MultiTypeFileStream fs1;
        BigT outB = new BigT(b.getName().concat("remfile"));
        Sort s = new Sort(attrType, (short) 4, attrSize, fs, new int[]{0}, new TupleOrder(TupleOrder.Ascending), MAXROWLABELSIZE, 134);
        Map tempMap, prevMap=null, retMap, tMap;
        while((tempMap = s.get_next())!=null) {
            if (prevMap == null) {
                prevMap = new Map();
                prevMap.mapCopy(tempMap);
                if (tempMap.getColumnLabel().equalsIgnoreCase(columnFilter)) {
                    prevMap = new Map();
                    tMap = traverseNewRow(s, tempMap.getRowLabel());
                    if(tMap == null) {
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
                if(tMap == null) {
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
