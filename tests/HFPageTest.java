package tests;

import java.io.*;
import java.util.Random;

import BigT.Map;
import BigT.MapIter;
import BigT.Mapfile;
import BigT.PredEval;
import BigT.Sort;
import BigT.Stream;
import diskmgr.Page;
import heap.*;
import iterator.SortException;
import global.*;

/**
 * Note that in JAVA, methods can't be overridden to be more private. Therefore,
 * the declaration of all private functions are now declared protected as
 * opposed to the private type in C++.
 */

class HFPageDriver extends TestDriver implements GlobalConst {

  private final static boolean OK = true;
  private final static boolean FAIL = false;

  public HFPageDriver() {
    super("streamtest");
  }

  protected boolean test1() {
    Map[] maps = new Map[12];
    byte[][] bytes = new byte[12][];
    Random x = new Random((int) 10);

    String[] rows = new String[maps.length / 4];

    try {
      for (int i = 0; i < maps.length / 4; i++) {
        rows[i] = "row" + i;
        for (int j = 0; j < 4; j++) {
          maps[i * 4 + j] = new Map();
          maps[i * 4 + j].setRowLabel(rows[i]);
          maps[i * 4 + j].setColumnLabel("Col" + x.nextInt(100));
          maps[i * 4 + j].setTimeStamp(i * 4 + j);
          maps[i * 4 + j].setValue(Integer.toString(i * 4 + j));
  
          bytes[i * 4 + j] = maps[i * 4 + j].getMapByteArray();
        }
      }

      for (int i = 0; i < maps.length; i++) {
        maps[i].print();
      }
      System.out.println("");
      
    } catch (Exception e) {
      e.printStackTrace();
      return false;
    }

    // HFPage currentDataPage = null;
    // Page apage = new Page();

    // PageId tmpId = null;

    // try {

    //   tmpId = SystemDefs.JavabaseBM.newPage(apage, 1);
    //   if (tmpId == null)
    //     throw new HFException(null, "can't new pae");
    //   currentDataPage = new HFPage();
    //   currentDataPage.init(tmpId, apage);
    //   int k = currentDataPage.batch_insert(bytes);

    //   for (int i = 0; i < k; i++) {
    //     Map m = new Map(currentDataPage.getRecord(new RID(tmpId, i)), 0);
    //     m.print();
    //     assert m.getColumnLabel().equals("Col" + i) : "Column mismatch in Map " + i;
    //     assert m.getRowLabel().equals("row" + rows[i]) : "Row mismatch in Map '" + rows[i] + "', Got: '" + m.getRowLabel() + "'";
    //     assert m.getTimeStamp() == i : "Timestamp mismatch in Map " + i;
    //     assert m.getValue().equals(Integer.toString(i)) : "Value mismatch in Map " + i;
    //   }

    // } catch (Exception e) {
    //   e.printStackTrace();
    //   return false;
    // }

    // AttrType[] attrType = { new AttrType(AttrType.attrString), new AttrType(AttrType.attrString),
    //     new AttrType(AttrType.attrInteger), new AttrType(AttrType.attrString) };
    // short[] attrSize = { MAXROWLABELSIZE, MAXCOLUMNLABELSIZE, MAXVALUESIZE };
    // MapIter iter = new MapIter(maps);

    // try {
    //   Sort st = new Sort(attrType, (short) 4, attrSize, iter, new int[] { 0, 1 }, new TupleOrder(TupleOrder.Ascending),
    //       MAXROWLABELSIZE, GlobalConst.NUMBUF / 8);
    //   Map m = st.get_next();
    //   while (m != null) {
    //     m.print();
    //     m = st.get_next();
    //   }
      
    // } catch (Exception e) {
    //   e.printStackTrace();
    //   return false;
    // }

    try {
      Mapfile f = new Mapfile("file_1");
      f.batch_insert(maps);
      Stream s = f.openStream();
      MID rid = new MID();

      Map m = s.getNext(rid);
      while (m != null) {
        m.print();
        m = s.getNext(rid);
      }


    } catch (Exception e) {
      e.printStackTrace();
      return false;
    }


    return true;
  }

  protected String testName () {
    return "Heap File";
  }
}

public class HFPageTest {

  public static void main (String argv[]) {

    HFPageDriver hd = new HFPageDriver();
    boolean dbstatus = false;
    try {
        dbstatus = hd.runTests();
    } catch (Exception e) {

    }

    if (!dbstatus) {
        System.err.println("HF Test failed.");
    }

    Runtime.getRuntime().exit(0);
  }
}

