package BigT;

import driver.FilterParser;
import global.AttrType;
import global.GlobalConst;
import global.TupleOrder;

public class RowSort implements GlobalConst {
    static AttrType[] attrType = {new AttrType(AttrType.attrString), new AttrType(AttrType.attrString), new AttrType(AttrType.attrInteger), new AttrType(AttrType.attrString)};
    static short[] attrSize = {MAXROWLABELSIZE, MAXCOLUMNLABELSIZE, MAXVALUESIZE};
    public static bigT rowSort(bigT b,String columnFilter, int num_pages) throws Exception {
        FileStream fs = new FileStream(b.getHf(), FilterParser.parseSingle(columnFilter,2, AttrType.attrString));
        LatestVersion lv = new LatestVersion(fs,num_pages);
        Sort s = new Sort(attrType, (short) 4, attrSize, lv, new int[]{3}, new TupleOrder(TupleOrder.Ascending), MAXROWLABELSIZE, 134);
        Map tempMap, tempWriteMap;
        bigT outB = new bigT("rowSort".concat(b.getName()),1);
        while ((tempMap = s.get_next())!=null){
            FileStream fsRow = new FileStream(b.getHf(), FilterParser.parseSingle(tempMap.getRowLabel(),1, AttrType.attrString));
            //System.out.print("Main map : ");
            //tempMap.print();
            while ((tempWriteMap = fsRow.get_next())!=null){
                //tempWriteMap.print();
                outB.insertMap(tempWriteMap.getMapByteArray());
            }
            fsRow.close();
        }
        s.close();
        lv.close();
        fs.close();
        fs = new FileStream(createRemFile(b,columnFilter,num_pages).getHf(), null);
        while ((tempWriteMap = fs.get_next())!=null){
            //tempWriteMap.print();
            outB.insertMap(tempWriteMap.getMapByteArray());
        }
        fs.close();
        return outB;
    }

    public static bigT createRemFile(bigT b, String columnFilter, int num_pages) throws Exception {
        FileStream fs = new FileStream(b.getHf(), null);
        FileStream fs1;
        bigT outB = new bigT(b.getName().concat("remfile"),1);
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
                fs1 = new FileStream(b.getHf(), FilterParser.parseSingle(prevMap.getRowLabel(), 1, AttrType.attrString));
                while ((retMap = fs1.get_next()) != null) {
                    outB.insertMap(retMap.getMapByteArray());
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
