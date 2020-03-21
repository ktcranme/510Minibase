package tests;

import java.io.*;
import java.util.HashMap;

import BigT.Map;
import BigT.MapPage;
import BigT.Mapfile;
import BigT.PredEval;
import BigT.Stream;

import heap.*;
import global.*;

/** Note that in JAVA, methods can't be overridden to be more private.
  Therefore, the declaration of all private functions are now declared
  protected as opposed to the private type in C++.
  */

class StreamDriver extends TestDriver implements GlobalConst
{

  private final static boolean OK = true;
  private final static boolean FAIL = false;

  private int choice = 100;
  private final static int reclen = 32;

  public StreamDriver () {
    super("streamtest");
  }
  
  public void setChoice(int c) {
    choice = c;
  }

  protected boolean test1 ()  {

    System.out.println ("\n  Test 1: Insert and scan fixed-size records\n");
    boolean status = OK;
    MID rid = new MID();
    Mapfile f = null;
    int rec_cnt = 0;

    System.out.println ("  - Create a heap file\n");
    try {
      f = new Mapfile("file_1");
    }
    catch (Exception e) {
      status = FAIL;
      System.err.println ("*** Could not create heap file\n");
      e.printStackTrace();
    }

    if ( status == OK && SystemDefs.JavabaseBM.getNumUnpinnedBuffers()
        != SystemDefs.JavabaseBM.getNumBuffers() ) {
      System.err.println ("*** The heap file has left pages pinned\n");
      status = FAIL;
        }

    if ( status == OK ) {
      System.out.println ("  - Add " + choice + " records to the file\n");
      for (int i =0; (i < choice) && (status == OK); i++) {

        //fixed length record
        Map m1 = new Map();
        try {
          m1.setRowLabel("row" + i);
          m1.setColumnLabel("col" + i);
          m1.setTimeStamp(i);
          m1.setValue(Integer.toString(i));

        } catch (Exception e) {
          status = FAIL;
          System.err.println ("*** Could not create heap file\n");
          e.printStackTrace();
        }

        try {
          rid = f.insertMap(m1);
        }
        catch (Exception e) {
          status = FAIL;
          System.err.println ("*** Error inserting record " + i + "\n");
          e.printStackTrace();
        }

        if ( status == OK && SystemDefs.JavabaseBM.getNumUnpinnedBuffers()
            != SystemDefs.JavabaseBM.getNumBuffers() ) {

          System.err.println ("*** Insertion left a page pinned\n");
          status = FAIL;
            }
      }

      try {
        if ( f.getRecCnt() != choice ) {
          status = FAIL;
          System.err.println ("*** File reports " + f.getRecCnt() + 
              " records, not " + choice + "\n");
        }
      }
      catch (Exception e) {
        status = FAIL;
        System.out.println (""+e);
        e.printStackTrace();
      }
    }

    // In general, a sequential scan won't be in the same order as the
    // insertions.  However, we're inserting fixed-length records here, and
    // in this case the scan must return the insertion order.

    Stream stream = null;

    if ( status == OK ) {	
      System.out.println ("  - Scan the records just inserted\n");

      try {
        stream = f.openStream();
      }
      catch (Exception e) {
        status = FAIL;
        System.err.println ("*** Error opening scan\n");
        e.printStackTrace();
      }

      if (status == OK && choice == 0 &&
        SystemDefs.JavabaseBM.getNumUnpinnedBuffers() != SystemDefs.JavabaseBM.getNumBuffers()) {
        System.err.println ("*** The heap-file scan has pinned a page despite having 0 records\n");
        status = FAIL;
      } else if ( status == OK && choice != 0 &&
        SystemDefs.JavabaseBM.getNumUnpinnedBuffers() == SystemDefs.JavabaseBM.getNumBuffers() ) {
        System.err.println ("*** The heap-file scan has not pinned the first page\n");
        status = FAIL;
      }
    }	

    if ( status == OK ) {
      int len, i = 0;
      Map m2 = new Map();

      boolean done = false;
      while (!done) { 
        try {
          m2 = stream.getNext(rid);
          if (m2 == null) {
            if (rec_cnt != choice) {
              status = FAIL;
              System.err.println ("*** Record count does not match inserted count!!! Found " + rec_cnt + " records!");
              break;
            }
            rec_cnt = 0;
            done = true;
            break;
          } else {
            rec_cnt++;
          }
        }
        catch (Exception e) {
          status = FAIL;
          e.printStackTrace();
        }

        if (status == OK && !done) {
          len = m2.getLength();
          if ( len != MAXROWLABELSIZE + MAXCOLUMNLABELSIZE + 4 + MAXVALUESIZE ) {
            System.err.println ("*** Record " + i + " had unexpected length " 
                + len + "\n");
            status = FAIL;
            break;
          }
          else if ( SystemDefs.JavabaseBM.getNumUnpinnedBuffers()
              == SystemDefs.JavabaseBM.getNumBuffers() ) {
            System.err.println ("On record " + i + ":\n");
            System.err.println ("*** The heap-file scan has not left its " +
                "page pinned\n");
            status = FAIL;
            break;
              }
          String name = ("record" + i );

          try {
            if( (!m2.getRowLabel().equals("row" + i))
                || (!m2.getColumnLabel().equals("col" + i))
                || (!m2.getValue().equals(Integer.toString(i)))
                || (m2.getTimeStamp() != i)) {
              System.err.println ("*** Record " + i
                  + " differs from what we inserted\n");
              System.err.println ("Row: " + m2.getRowLabel()
                  + " should be " + "row" + i + "\n");
              System.err.println ("Column: "+ m2.getColumnLabel()
                  + " should be " + "col" + i + "\n");
              System.err.println ("Timestamp: " + m2.getTimeStamp()
                  + " should be " + i + "\n");
              System.err.println ("Value: " + m2.getValue()
                  + " should be " + Integer.toString(i) + "\n");
              status = FAIL;
              break;
            } else {
              //System.out.println(m2.getRowLabel() + ", " + m2.getColumnLabel() + ", " + Integer.toString(m2.getTimeStamp()) + ", " + m2.getValue());
            }
          } catch (Exception e) {
            status = FAIL;
            System.out.println (""+e);
            e.printStackTrace();
          }
        } 	
        ++i;
      }

      //If it gets here, then the scan should be completed
      if (status == OK) {
        if ( SystemDefs.JavabaseBM.getNumUnpinnedBuffers() 
            != SystemDefs.JavabaseBM.getNumBuffers() ) {
          System.err.println ("*** The heap-file scan has not unpinned " + 
              "its page after finishing\n");
          status = FAIL;
            }
        else if ( i != (choice) )
        {
          status = FAIL;

          System.err.println ("*** Scanned " + i + " records instead of "
              + choice + "\n");
        }
      }	
    }

    if ( status == OK )
      System.out.println ("  Test 1 completed successfully.\n");

    return status; 
  }

  protected boolean test2 () {

    System.out.println ("\n  Test 2: Delete fixed-size records\n");
    boolean status = OK;
    Stream stream = null;
    MID rid = new MID();
    Mapfile f = null;
    int rec_cnt = 0;

    System.out.println ("  - Open the same heap file as test 1\n");
    try {
      f = new Mapfile("file_1");
    }
    catch (Exception e) {
      status = FAIL;
      System.err.println (" Could not open heapfile");
      e.printStackTrace();
    }

    if ( status == OK ) {
      System.out.println ("  - Delete half the records\n");
      try {
        stream = f.openStream();
      }
      catch (Exception e) {
        status = FAIL;
        System.err.println ("*** Error opening scan\n");
        e.printStackTrace();
      }
    }

    if ( status == OK ) {
      int len, i = 0;
      Map m1 = new Map();
      boolean done = false;

      while (!done) { 
        try {
          m1 = stream.getNext(rid);
          if (m1 == null) {
            if (rec_cnt != choice) {
              status = FAIL;
              System.err.println ("*** Record count before deletion does not match!!! Found " + rec_cnt + " records!");
              break;
            }
            done = true;
            rec_cnt = 0;
          } else {
            //System.out.println(m1.getRowLabel() + ", " + m1.getColumnLabel() + ", " + Integer.toString(m1.getTimeStamp()) + ", " + m1.getValue());
            rec_cnt++;
          }
        }
        catch (Exception e) {
          status = FAIL;
          e.printStackTrace();
          return false;
        }

        if (!done && status == OK) {
          boolean odd = true;
          if ( i % 2 == 1 ) odd = true;
          if ( i % 2 == 0 ) odd = false;
          if ( odd )  {       // Delete the odd-numbered ones.
            try {
              status = f.deleteMap( rid );
            }
            catch (Exception e) {
              status = FAIL;
              System.err.println ("*** Error deleting record " + i + "\n");
              e.printStackTrace();
              break;
            }
          }
        }
        ++i;
      }
    }

    stream.closestream();	//  destruct scan!!!!!!!!!!!!!!!
    stream = null;

    if ( status == OK && SystemDefs.JavabaseBM.getNumUnpinnedBuffers() 
        != SystemDefs.JavabaseBM.getNumBuffers() ) {

      System.out.println ("\nt2: in if: Number of unpinned buffers: " 
          + SystemDefs.JavabaseBM.getNumUnpinnedBuffers()+ "\n");
      System.err.println ("t2: in if: getNumbfrs: "+SystemDefs.JavabaseBM.getNumBuffers() +"\n"); 

      System.err.println ("*** Deletion left a page pinned\n");
      status = FAIL;
        }

    if ( status == OK ) {
      System.out.println ("  - Scan the remaining records\n");
      try {
        stream = f.openStream();
      }
      catch (Exception e ) {
        status = FAIL;
        System.err.println ("*** Error opening scan\n");
        e.printStackTrace();
      }
    }

    if ( status == OK ) {
      int len, i = 0;
      Map m2 = new Map();
      boolean done = false;

      while ( !done ) {
        try {
          m2 = stream.getNext(rid);
          if (m2 == null) {
            if (rec_cnt != (int) java.lang.Math.ceil(choice/(float)2)) {
              status = FAIL;
              System.err.println ("*** Record count after deletion does not match!!! Found " + rec_cnt + " records!");
              break;
            }
            done = true;
          } else {
            //System.out.println(m2.getColumnLabel() + ", " + m2.getRowLabel() + ", " + m2.getTimeStamp() + ", " + m2.getValue());
            rec_cnt++;
          }
        }
        catch (Exception e) {
          status = FAIL;
          e.printStackTrace();
        }

        if (!done && status == OK) {

          try {
            if( (!m2.getRowLabel().equals("row" + i))
                || (!m2.getColumnLabel().equals("col" + i))
                || (!m2.getValue().equals(Integer.toString(i)))
                || (m2.getTimeStamp() != i)) {
              System.err.println ("*** Record " + i
                  + " differs from what we inserted\n");
              System.err.println ("Row: " + m2.getRowLabel()
                  + " should be " + "row" + i + "\n");
              System.err.println ("Column: "+ m2.getColumnLabel()
                  + " should be " + "col" + i + "\n");
              System.err.println ("Timestamp: " + m2.getTimeStamp()
                  + " should be " + i + "\n");
              System.err.println ("Value: " + m2.getValue()
                  + " should be " + Integer.toString(i) + "\n");
            status = FAIL;
            break;
              }
          } catch (Exception e) {
            System.err.println(e);
            status = FAIL;
            break;
          }

          i += 2;     // Because we deleted the odd ones...
        }
      }
    }

    if ( status == OK )
      System.out.println ("  Test 2 completed successfully.\n");
    return status; 

  }

  protected boolean test3 () {

    System.out.println ("\n  Test 3: Update fixed-size records\n");
    boolean status = OK;
    Stream stream = null;
    MID rid = new MID();
    Mapfile f = null; 
    int rec_cnt = 0;

    System.out.println ("  - Open the same heap file as tests 1 and 2\n");
    try {
      f = new Mapfile("file_1");
    }
    catch (Exception e) {
      status = FAIL;
      System.err.println ("*** Could not create heap file\n");
      e.printStackTrace();
    }

    if ( status == OK ) {
      System.out.println ("  - Change the records\n");
      try {
        stream = f.openStream();
      }
      catch (Exception e) {
        status = FAIL;
        System.err.println ("*** Error opening scan\n");
        e.printStackTrace();
      }
    }

    if ( status == OK ) {

      int len, i = 0;
      Map m = new Map();
      boolean done = false;

      while ( !done ) {
        try {
          m = stream.getNext(rid);
          if (m == null) {
            if (rec_cnt != (int) java.lang.Math.ceil(choice/(float)2)) {
              status = FAIL;
              System.err.println ("*** Record count does not match inserted count!!! Found " + rec_cnt + " records!");
              break;
            }
            rec_cnt = 0;
            done = true;
          } else {
            //System.out.println(m.getColumnLabel() + ", " + m.getRowLabel() + ", " + m.getTimeStamp() + ", " + m.getValue());
            rec_cnt++;
          }
        }
        catch (Exception e) {
          status = FAIL;
          e.printStackTrace();
          return false;
        }

        if (!done && status == OK) {

          /**
           * The idea here is that the MAP pointed to by rid must not change
           * ... Unless it is very old and has to be removed because of the update.
           */
          HashMap<Integer, Map> h = new HashMap<Integer, Map>();
          h.put(rid.slotNo, new Map(m));

          // Update multiple times to check versioning
          for (int k = 0; k < 6; k++) {
            Map newMap = null; 
            try {
              newMap = new Map (m.getMapByteArray(),0); 
              newMap.setTimeStamp(10 * i + k);
              newMap.setValue(Integer.toString(10 * i + k));
            }
            catch (Exception e) {
              status = FAIL;
              System.err.println (""+e);
              e.printStackTrace();
            }
            try {
              Map mk = new Map();
              MID tempMid = f.updateMap(rid, newMap, mk);

              if (tempMid.isReused) {
                // Verify if the map ejected was actually the same as the one at tempMid.slotNo
                assert h.get(tempMid.slotNo).getTimeStamp() == mk.getTimeStamp() : "The ejected Map is not the same as expected";
                assert h.get(tempMid.slotNo).getValue().equals(mk.getValue()) : "The ejected Map is not the same as expected";
              }

              h.put(tempMid.slotNo, new Map(newMap));

              // Verify if all the maps we saved so far are still refered to by the same MIDs
              for(java.util.Map.Entry<Integer, Map> entry : h.entrySet()) {
                Integer slotNo = entry.getKey();
                Map v = entry.getValue();
            
                Map tempMap = f.getMap(new MID(tempMid.pageNo, slotNo));
                assert tempMap.getRowLabel().equals(v.getRowLabel()) : "MID must be constant! Row mismatch!";
                assert tempMap.getColumnLabel().equals(v.getColumnLabel()) : "MID must be constant! Column mismatch!";
                assert tempMap.getValue().equals(v.getValue()) : "MID must be constant! Value mismatch!";
                assert tempMap.getTimeStamp() == v.getTimeStamp() : "MID must be constant! Timestamp mismatch!";
              }
            }
            catch (Exception e) {
              status = FAIL;
              e.printStackTrace();
            }

            if ( status != OK ) {
              System.err.println ("*** Error updating record " + i + "\n");
              break;
            }
          }
          i += 2;     // Recall, we deleted every other record above.
        }
      }
    }

    stream = null;

    if ( status == OK && SystemDefs.JavabaseBM.getNumUnpinnedBuffers() 
        != SystemDefs.JavabaseBM.getNumBuffers() ) {


      System.out.println ("t3, Number of unpinned buffers: " 
          + SystemDefs.JavabaseBM.getNumUnpinnedBuffers()+ "\n");
      System.err.println ("t3, getNumbfrs: "+SystemDefs.JavabaseBM.getNumBuffers() +"\n"); 

      System.err.println ("*** Updating left pages pinned\n");
      status = FAIL;
        }

    if ( status == OK ) {
      System.out.println ("  - Check that the updates are really there\n");
      try {
        stream = f.openStream();
        if (f.getMapCnt() != (int) java.lang.Math.ceil(choice/(float)2) * 3) {
          status = FAIL;
          System.err.println ("*** File reports " + f.getMapCnt() + " records, expected " + (int) java.lang.Math.ceil(choice/(float)2) * 3);
          return false;
        }
      }
      catch (Exception e) {
        status = FAIL;
        e.printStackTrace();
      }
      if (status == FAIL) {
        System.err.println ("*** Error opening scan\n");
      }
    }

    if ( status == OK ) {
      int len, i = 0;
      Map m = new Map(); 
      Map m2 = new Map(); 
      boolean done = false;

      while ( !done ) {
        try {
          m = stream.getNext(rid);
          if (m == null) {
            // Because we created two versions of the map now
            if (rec_cnt != (int) java.lang.Math.ceil(choice/(float)2) * 3) {
              status = FAIL;
              System.err.println ("*** Record count does not match inserted count!!! Found " + rec_cnt + " records!");
              break;
            }
            rec_cnt = 0;
            done = true;
            break;
          } else {
            //System.out.println(m.getColumnLabel() + ", " + m.getRowLabel() + ", " + m.getTimeStamp() + ", " + m.getValue());
            // m.print();
            rec_cnt++;
          }
        }
        catch (Exception e) {
          status = FAIL;
          e.printStackTrace();
        }

        if (!done && status == OK) {
          // While we're at it, test the getRecord method too.
          try {
            m2 = f.getMap( rid );
          }
          catch (Exception e) {
            status = FAIL;
            System.err.println ("*** Error getting record " + i + "\n");
            e.printStackTrace();
            break;
          }

          // i += 2;
          if (m.getVersionNo() == 2) {
            try {
              if( !m.getValue().equals(Integer.toString(i * 10 + 4)) || !m2.getValue().equals(Integer.toString(i * 10 + 4))) {
                System.err.println ("*** Record " + i
                    + " differs from our update\n");
                System.err.println ("m.value: "+ m.getValue()
                    + " should be " + Integer.toString(i * 10 + 4) + "\n");
                System.err.println ("m2.value: "+ m2.getValue()
                    + " should be " + Integer.toString(i * 10 + 4) + "\n");
                status = FAIL;
                break;
              }
            } catch (Exception e) {
                status = FAIL;
                break;
            }
            i += 2;     // Because we deleted the odd ones...
          } else if (m.getVersionNo() == 1) {
            try {
              if( !m.getValue().equals(Integer.toString(i * 10 + 3)) || !m2.getValue().equals(Integer.toString(i * 10 + 3))) {
                System.err.println ("*** Record " + i
                    + " differs from our update\n");
                System.err.println ("m.value: "+ m.getValue()
                    + " should be " + Integer.toString(i * 10 + 3) + "\n");
                System.err.println ("m2.value: "+ m2.getValue()
                    + " should be " + Integer.toString(i * 10 + 3) + "\n");
                status = FAIL;
                break;
              }
            } catch (Exception e) {
                status = FAIL;
                break;
            }
          } else {
            try {
              if( !m.getValue().equals(Integer.toString(i * 10 + 5)) || !m2.getValue().equals(Integer.toString(i * 10 + 5))) {
                System.err.println ("*** Record " + i
                    + " differs from our update\n");
                System.err.println ("m.value: "+ m.getValue()
                    + " should be " + Integer.toString(i * 10 + 5) + "\n");
                System.err.println ("m2.value: "+ m2.getValue()
                    + " should be " + Integer.toString(i * 10 + 5) + "\n");
                status = FAIL;
                break;
              }
            } catch (Exception e) {
                status = FAIL;
                break;
            }
            // i += 2;     // Because we deleted the odd ones...
          }

        }
      }
    }

    if ( status == OK )
      System.out.println ("  Test 3 completed successfully.\n");
    return status; 

  }

  protected boolean test4() {
    System.out.println("Running Test 4");

    boolean status = OK;
    Stream stream = null;
    MID rid = new MID();
    Mapfile f = null;

    System.out.println ("  - Open the same heap file as tests 1 and 2\n");
    try {
      f = new Mapfile("file_1");

      if (f.getMapCnt() < 8)
        return true;

      stream = f.openStream();
      System.out.println("Has: " + f.getMapCnt() + " Records");
    }
    catch (Exception e) {
      status = FAIL;
      System.err.println ("*** Could not create heap file\n");
      e.printStackTrace();
    }

    Map map = null;
    try {
      map = new Map();
      map.setColumnLabel("col8");
      map.setRowLabel("row8");
      map.setTimeStamp(8);
      map.setValue("8");

      MapPage mp = f.naiveSearch(map);
      assert mp != null : "Expected to find the record!";

      map.setColumnLabel("col1");
      map.setRowLabel("row1");
      map.setTimeStamp(1);
      map.setValue("1");

      mp = f.naiveSearch(map);
      assert mp == null : "Expected to not find the record!";

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

public class StreamTest {

  public static void main (String argv[]) {

    StreamDriver hd = new StreamDriver();
    boolean dbstatus = false;

    int[] sizes = {0, 50, 100, 480, 179, 366, 29};

    for (int size : sizes) {
      hd.setChoice(size);
      try {
        dbstatus = hd.runTests();
        if (!dbstatus) {
          System.err.println ("Error encountered during buffer manager tests:\n");
          Runtime.getRuntime().exit(1);
        }
      } catch (Exception e) {
        break;
      }
    }

    Runtime.getRuntime().exit(0);
  }
}

