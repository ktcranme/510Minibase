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
        return outB;
    }
}
