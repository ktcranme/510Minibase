package storage;

import btree.AddFileEntryException;
import btree.ConstructPageException;
import btree.GetFileEntryException;
import global.AttrType;
import global.GlobalConst;
import global.SystemDefs;
import global.TupleOrder;
import heap.*;
import BigT.Sort;

import BigT.Map;

import java.io.IOException;
import java.util.List;

public class BigDB implements GlobalConst {
    static AttrType[] attrType = {new AttrType(AttrType.attrString), new AttrType(AttrType.attrString), new AttrType(AttrType.attrInteger), new AttrType(AttrType.attrString)};
    static short[] attrSize = {MAXROWLABELSIZE, MAXCOLUMNLABELSIZE, MAXVALUESIZE};

    public static void getCounts(int num_buf) throws Exception {
        List<String> names = SystemDefs.JavabaseDB.bigTlist();
        mapCount(names);
        rowCount(names,num_buf);
        columnCount(names,num_buf);
    }

    public static void mapCount(List<String> names) throws HFDiskMgrException, HFException, IOException, ConstructPageException, AddFileEntryException, GetFileEntryException, HFBufMgrException, InvalidTupleSizeException, InvalidSlotNumberException {
        BigT b;
        int count = 0;
        for(String bigT:names){
            b = new BigT(bigT);
            count += b.getMapCount();
            b.close();
        }
        System.out.println("Map Count: "+ count);
    }

    public static void rowCount(List<String> names, int num_buf) throws Exception {
        Sort st = null;
        BigDBIterator bdb = null;

        try {
            bdb = new BigDBIterator(names);
            st = new Sort(attrType, (short) 4, attrSize, bdb, new int[]{0}, new TupleOrder(TupleOrder.Ascending), MAXROWLABELSIZE, (int)(num_buf * 0.4));
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
            bdb.close();
            System.out.println("Row Count: "+ c);
        } catch (Exception e) {
            e.printStackTrace();
            if (bdb != null) bdb.close();
            if (st != null) st.close();
        }
    }

    public static void columnCount(List<String> names, int num_buf) throws Exception {
        BigDBIterator bdb = null;
        Sort st = null;

        try {
            bdb= new BigDBIterator(names);
            st = new Sort(attrType, (short) 4, attrSize, bdb, new int[]{1}, new TupleOrder(TupleOrder.Ascending), MAXCOLUMNLABELSIZE, (int)(num_buf * 0.4));
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
            bdb.close();
            System.out.println("Column Count: "+ c);
        } catch (Exception e) {
            e.printStackTrace();
            if (bdb != null) bdb.close();
            if (st != null) st.close();
        }
    }
}
