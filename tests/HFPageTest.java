package tests;

import java.io.*;

import BigT.Map;
import BigT.Mapfile;
import BigT.PredEval;
import BigT.Stream;
import diskmgr.Page;
import heap.*;
import global.*;

/** Note that in JAVA, methods can't be overridden to be more private.
  Therefore, the declaration of all private functions are now declared
  protected as opposed to the private type in C++.
  */

class HFPageDriver extends TestDriver implements GlobalConst
{

  private final static boolean OK = true;
  private final static boolean FAIL = false;

  public HFPageDriver () {
    super("streamtest");
  }
  
  protected boolean test1 ()  {
    Map[] maps = new Map[6];
    byte[][] bytes = new byte[6][];

    try {
        for (int i = 0; i < maps.length; i++) {
            maps[i] = new Map();
            maps[i].setRowLabel("row" + i);
            maps[i].setColumnLabel("Col" + i);
            maps[i].setTimeStamp(i);
            maps[i].setValue(Integer.toString(i));

            bytes[i] = maps[i].getMapByteArray();
        }
    } catch (Exception e) {
        e.printStackTrace();
        return false;
    }

    HFPage currentDataPage = null;
    Page apage = new Page();

    PageId tmpId = null;

    try {

        tmpId = SystemDefs.JavabaseBM.newPage(apage, 1);
        if (tmpId == null)
            throw new HFException(null, "can't new pae");
        currentDataPage = new HFPage();
        currentDataPage.init(tmpId, apage);
        int k = currentDataPage.batch_insert(bytes);

        for (int i = 0; i < k; i++) {
            Map m = new Map(currentDataPage.getRecord(new RID(tmpId, i)), 0);
            m.print();
            assert m.getColumnLabel().equals("Col" + i) : "Column mismatch in Map " + i;
            assert m.getRowLabel().equals("row" + i) : "Row mismatch in Map " + i;
            assert m.getTimeStamp() == i : "Timestamp mismatch in Map " + i;
            assert m.getValue().equals(Integer.toString(i)) : "Value mismatch in Map " + i;
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

