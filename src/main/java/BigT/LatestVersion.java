package BigT;

import global.AttrType;
import global.GlobalConst;
import global.TupleOrder;
import index.IndexException;
import iterator.*;

import java.io.IOException;

public class LatestVersion extends Iterator implements GlobalConst {
    Iterator it;
    Map currMap;
    int num_pages;
    AttrType[] attrType = {new AttrType(AttrType.attrString), new AttrType(AttrType.attrString), new AttrType(AttrType.attrInteger), new AttrType(AttrType.attrString)};
    short[] attrSize = {MAXROWLABELSIZE, MAXCOLUMNLABELSIZE, MAXVALUESIZE};

    public LatestVersion(Iterator am, int buff) throws IOException, SortException {
        currMap = null;
        it = new Sort(attrType, (short) 4, attrSize, am, new int[]{0,1,2}, new TupleOrder(TupleOrder.Ascending), MAXROWLABELSIZE, 134);
    }

    public Map get_next() throws Exception {
        Map tempMap, retMap=null;
            while((tempMap = it.get_next()) != null){
                if(currMap == null || (tempMap.getRowLabel().equalsIgnoreCase(currMap.getRowLabel()) && tempMap.getColumnLabel().equalsIgnoreCase(currMap.getColumnLabel()))){
                    currMap = new Map();
                    currMap.mapCopy(tempMap);
                } else {
                    retMap = new Map();
                    retMap.mapCopy(currMap);
                    currMap = new Map();
                    currMap.mapCopy(tempMap);
                    break;
                }
            }
            if(tempMap==null) {
                if(currMap!=null) {
                    retMap = new Map();
                    retMap.mapCopy(currMap);
                    currMap = null;
                } else {
                    retMap= null;
                }
            }
            return retMap;
    }

    public void close() throws IOException, JoinsException, SortException, IndexException {
        if (!closeFlag) {
            it.close();
            closeFlag = true;
        }
    }
}
