package tests;

import java.io.IOException;

import BigT.FileStream;
import BigT.Map;
import BigT.Stream;
import global.AttrOperator;
import global.AttrType;
import global.GlobalConst;
import global.MID;
import global.SystemDefs;
import heap.Heapfile;
import iterator.CondExpr;
import iterator.FldSpec;
import iterator.RelSpec;

class FileStreamDriver extends TestDriver implements GlobalConst {

    public FileStreamDriver() {
        super("filestreamtest");
    }

    protected String testName() {
        return "File Stream";
    }

    Heapfile createDummyFile(String filename) {
        Heapfile f = null;
        try {
            f = new Heapfile(filename);
        } catch (Exception e) {
            System.err.println("*** Could not create heap file\n");
            e.printStackTrace();
        }
        return f;
    }

    boolean inputDummyDataToFile(Heapfile f, int numRecords) {
        System.out.println("  - Add " + numRecords + " records to the file\n");
        MID rid = null;
        for (int i = 0; i < numRecords; i++) {

            // fixed length record
            Map m1 = new Map();
            try {
                m1.setRowLabel("row" + i);
                m1.setColumnLabel("col" + i);
                m1.setTimeStamp(i);
                m1.setValue(Integer.toString(i));

            } catch (Exception e) {
                System.err.println("*** Could not create map\n");
                e.printStackTrace();
                return false;
            }

            try {
                rid = f.insertMap(m1);
            } catch (Exception e) {
                System.err.println("*** Error inserting record " + i + "\n");
                e.printStackTrace();
                return false;
            }

            if (!hasNoPagesLeftBehind())
                return false;
        }

        try {
            if (f.getRecCnt() != numRecords) {
                System.err.println("*** File reports " + f.getRecCnt() + " records, not " + numRecords + "\n");
                return false;
            }
        } catch (Exception e) {
            System.out.println("" + e);
            e.printStackTrace();
            return false;
        }

        return true;
    }

    boolean hasPagesLeftBehind() {
        if (SystemDefs.JavabaseBM.getNumUnpinnedBuffers() != SystemDefs.JavabaseBM.getNumBuffers()) {
            return true;
        }
        System.err.println("*** The heap file should have left pages pinned\n");
        return false;
    }

    boolean hasNoPagesLeftBehind() {
        if (SystemDefs.JavabaseBM.getNumUnpinnedBuffers() == SystemDefs.JavabaseBM.getNumBuffers()) {
            return true;
        }
        System.err.println("*** The heap file should have left no pages pinned\n");
        return false;
    }

    boolean checkDummyRecord(Map m2, int i) {
        try {
            if ((!m2.getRowLabel().equals("row" + i)) || (!m2.getColumnLabel().equals("col" + i))
                    || (!m2.getValue().equals(Integer.toString(i))) || (m2.getTimeStamp() != i)) {
                System.err.println("*** Record " + i + " differs from what we inserted\n");
                System.err.println("Row: " + m2.getRowLabel() + " should be " + "row" + i + "\n");
                System.err.println("Column: " + m2.getColumnLabel() + " should be " + "col" + i + "\n");
                System.err.println("Timestamp: " + m2.getTimeStamp() + " should be " + i + "\n");
                System.err.println("Value: " + m2.getValue() + " should be " + Integer.toString(i) + "\n");
                return false;
            } else {
                // System.out.println(m2.getRowLabel() + ", " + m2.getColumnLabel() + ", " +
                // Integer.toString(m2.getTimeStamp()) + ", " + m2.getValue());
            }
        } catch (Exception e) {
            System.out.println("Error in accessing Map attributes: " + e);
            e.printStackTrace();
            return false;
        }
        return true;
    }

    protected Heapfile setup(int choice, String filename) {
        System.out.println("\n  Insert and verify fixed-size records.\n");
        MID rid = new MID();
        Heapfile f = null;
        int rec_cnt = 0;

        System.out.println("  - Create a heap file\n");

        f = createDummyFile(filename);
        if (!hasNoPagesLeftBehind() || f == null)
            return null;
        if (!inputDummyDataToFile(f, choice))
            return null;

        // In general, a sequential scan won't be in the same order as the
        // insertions. However, we're inserting fixed-length records here, and
        // in this case the scan must return the insertion order.

        Stream stream = null;
        System.out.println("  - Scan the records just inserted\n");

        try {
            stream = f.openStream();
        } catch (Exception e) {
            System.err.println("*** Error opening stream\n");
            e.printStackTrace();
            return null;
        }

        if ((choice == 0 && !hasNoPagesLeftBehind()) || (choice != 0 && !hasPagesLeftBehind())) {
            return null;
        }

        int i = 0;
        Map m2 = new Map();

        boolean done = false;

        while (!done) {
            try {
                m2 = stream.getNext(rid);
                done = (m2 == null);
                rec_cnt += m2 == null ? 0 : 1;
            } catch (Exception e) {
                e.printStackTrace();
                return null;
            }

            if (i > choice) {
                System.err.println("*** Scanned " + i + " records instead of " + choice + "\n");
                return null;
            }

            if (done) {
                if (rec_cnt != choice) {
                    System.err.println(
                            "*** Record count does not match inserted count!!! Found " + rec_cnt + " records!");
                    return null;
                }
                break;
            }

            if (m2.getLength() != MAXROWLABELSIZE + MAXCOLUMNLABELSIZE + 4 + MAXVALUESIZE) {
                System.err.println("*** Record " + i + " had unexpected length " + m2.getLength() + "\n");
                return null;
            } else if (!hasPagesLeftBehind()) {
                return null;
            }

            if (!checkDummyRecord(m2, i)) {
                return null;
            }

            i++;
        }

        if (!hasNoPagesLeftBehind()) {
            return null;
        }

        return f;
    }

    protected boolean test1() {
        System.out.println ("\nTest 1: Filter records with timestamp > 70 && timestamp < 40\n");

        Heapfile f = setup(100, "file_1");

        // ROW LABEL == "row0" AND (VALUE > 70 OR VALUE < 40)
        CondExpr[] expr2 = new CondExpr[1];

        expr2[0] = new CondExpr();
        expr2[0].op = new AttrOperator(AttrOperator.aopGT);
        expr2[0].next = null;
        expr2[0].type1 = new AttrType(AttrType.attrSymbol);

        expr2[0].operand1.symbol = new FldSpec(new RelSpec(RelSpec.outer), 3);
        expr2[0].type2 = new AttrType(AttrType.attrInteger);
        expr2[0].operand2.integer = 70;

        expr2[0].next = new CondExpr();
        expr2[0].next.op = new AttrOperator(AttrOperator.aopLT);
        expr2[0].next.next = null;
        expr2[0].next.type1 = new AttrType(AttrType.attrSymbol); // rating
        expr2[0].next.operand1.symbol = new FldSpec(new RelSpec(RelSpec.outer), 3);
        expr2[0].next.type2 = new AttrType(AttrType.attrInteger);
        expr2[0].next.operand2.integer = 40;

        Map m = null;

        FileStream fs;
        try {
            fs = new FileStream("file_1", expr2);
        } catch (Exception e) {
            e.printStackTrace();
            System.out.println("Could not create filestream!");
            return false;
        }

        try {
            m = fs.get_next();
            while (m != null) {
                if (m.getTimeStamp() > 40 && m.getTimeStamp() < 70) {
                    m.print();
                    System.err.println("Filtering isnt respecting condition!");
                    return false;
                }
                m = fs.get_next();
            }
        } catch (Exception e) {
            e.printStackTrace();
            System.out.println("Could not read from filestream!");
            return false;
        }

        System.out.println ("Test 1 completed successfully.\n");
        return true;
    }
}

public class FileStreamTest {
    public static void main(String argv[]) {
        FileStreamDriver fs = new FileStreamDriver();
        boolean status = false;
        try {
            status = fs.runTests();
        } catch (IOException e) {
            e.printStackTrace();
        }

        if (status != true) {
            System.out.println("Error ocurred during test");
            Runtime.getRuntime().exit(1);
        }

        System.out.println("test completed successfully");
        Runtime.getRuntime().exit(0);
    }
}
