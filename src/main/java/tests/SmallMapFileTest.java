package tests;

import java.io.IOException;
import java.util.Random;

import BigT.Map;
import BigT.Stream;
import BigT.VMapfile;
import global.GlobalConst;
import global.MID;
import global.SystemDefs;
import heap.HFBufMgrException;
import heap.InvalidSlotNumberException;
import heap.InvalidTupleSizeException;
import storage.SmallMap;
import storage.SmallMapFile;

class SmallMapFileTestDriver extends TestDriver implements GlobalConst {
    public Integer[] randoms = new Integer[100];

    public SmallMapFileTestDriver() {
        super("Small Map File Test");
    }

    protected String testName() {
        return "Small Map File";
    }

    protected boolean test1() {
        System.out.println ("\n  Test 1: Insert and scan fixed-size records\n");

        Random rand = new Random();

        for (int i = 0; i < 100; i++) {
            randoms[i] = rand.nextInt(1000);
        }

        MID rid = new MID();
        SmallMapFile f = null;

        System.out.println ("  - Create a heap file\n");
        try {
            f = new SmallMapFile("file_1", "row1", 1);
        } catch (Exception e) {
            System.err.println ("*** Could not create heap file\n");
            e.printStackTrace();
            return false;
        }

        assert SystemDefs.JavabaseBM.getNumUnpinnedBuffers() == SystemDefs.JavabaseBM.getNumBuffers() : "*** The heap-file scan has left pinned pages " + SystemDefs.JavabaseBM.getNumUnpinnedBuffers() + "/" + SystemDefs.JavabaseBM.getNumBuffers();

        for (int i = 0; i < 100; i++) {
            //fixed length record
            Map m1 = new Map();
            try {
                m1.setRowLabel("row1");
                m1.setColumnLabel("col" + randoms[i]);
                m1.setTimeStamp(randoms[i]);
                m1.setValue(Integer.toString(randoms[i]));

                f.insertMap(m1);
            } catch (Exception e) {
                System.err.println ("*** Could not make map");
                e.printStackTrace();
                return false;
            }
        }

        try {
            assert f.getRecCnt() == 100 : "*** File reports " + f.getRecCnt() + " records, not " + 100;
        } catch (Exception e) {
            System.err.println ("*** Could not create heap file\n");
            e.printStackTrace();
            return false;
        }

        Stream stream = null;
        System.out.println ("  - Scan the records just inserted\n");

        try {
            stream = f.openStream();
        } catch (Exception e) {
            System.err.println ("*** Error opening scan\n");
            e.printStackTrace();
            return false;
        }

        Map map = new Map();
        int count = 0;
        while (map != null) {
            try {
                assert SystemDefs.JavabaseBM.getNumUnpinnedBuffers() != SystemDefs.JavabaseBM.getNumBuffers() : "*** The heap-file scan has not pinned any pages";
                map = stream.getNext(rid);

                assert map.getRowLabel().equals("row1") : "Got row label " + map.getRowLabel() + " but expected row1";
                assert map.getColumnLabel().equals("col" + randoms[count]) : "Got row label " + map.getColumnLabel() + " but expected col" + randoms[count];
                assert map.getTimeStamp() == randoms[count] : "Got row label " + map.getTimeStamp() + " but expected " + randoms[count];
                assert Integer.parseInt(map.getValue()) == randoms[count] : "Got value " + map.getValue() + " but expected " + randoms[count];

                if (map == null)
                    break;

//                map.print();
                count++;
            } catch (Exception e) {
                e.printStackTrace();
                stream.closestream();
                return false;
            }
        }

        stream.closestream();
        assert count == 100 : "Returned records from stream doesnt match insert count!";

        try {
            f.test();
        } catch (Exception e) {
            e.printStackTrace();
            return false;
        }

        assert SystemDefs.JavabaseBM.getNumUnpinnedBuffers() == SystemDefs.JavabaseBM.getNumBuffers() : "*** The heap-file scan has left pinned pages";

        System.out.println ("  Test 1 completed successfully.\n");
        return true;
    }

    protected boolean test2 () {
        System.out.println ("\n  Test 2: Delete fixed-size records\n");
        Stream stream = null;
        MID rid = new MID();
        SmallMapFile f = null;
        int rec_cnt = 0;

        System.out.println ("  - Open the same heap file as test 1\n");
        try {
            f = new SmallMapFile("file_1", "row1", 1);
        } catch (Exception e) {
            System.err.println ("*** Could not create heap file\n");
            e.printStackTrace();
            return false;
        }

        Map map = null;
        System.out.println ("  - Delete half the records\n");
        try {
            stream = f.openStream();
            map = stream.getNext(rid);
        }
        catch (Exception e) {
            System.err.println ("*** Error opening scan\n");
            e.printStackTrace();
            return false;
        }


        int count = 0;
        while (map != null) {
            try {
                count++;

                if (count % 2 != 0) {
                    f.deleteMap(rid);
                }

                map = stream.getNext(rid);
            } catch (Exception e) {
                e.printStackTrace();
                stream.closestream();
                return false;
            }
        }

        stream.closestream();
        assert count == 100 : "*** Record count before deletion does not match!!! Found " + count + " records!";
        assert SystemDefs.JavabaseBM.getNumUnpinnedBuffers() == SystemDefs.JavabaseBM.getNumBuffers() : "*** The heap-file scan has left pinned pages";

        System.out.println ("  - Scan the remaining records\n");
        try {
            stream = f.openStream();
            map = stream.getNext(rid);
        }
        catch (Exception e) {
            System.err.println ("*** Error opening scan\n");
            e.printStackTrace();
            return false;
        }

        count = 0;
        while (map != null) {
            try {
                count += 2;

                assert map.getRowLabel().equals("row1") : "Got row label " + map.getRowLabel() + " but expected row1";
                assert map.getColumnLabel().equals("col" + randoms[count]) : "Got row label " + map.getColumnLabel() + " but expected col" + randoms[count];
                assert map.getTimeStamp() == randoms[count] : "Got row label " + map.getTimeStamp() + " but expected " + randoms[count];
                assert Integer.parseInt(map.getValue()) == randoms[count] : "Got value " + map.getValue() + " but expected " + randoms[count];

                map = stream.getNext(rid);
            } catch (Exception e) {
                e.printStackTrace();
                stream.closestream();
                return false;
            }
        }

        stream.closestream();

        try {
            f.test();
        } catch (Exception e) {
            e.printStackTrace();
            return false;
        }

        assert count == 100 : "*** Record count before deletion does not match!!! Found " + count + " records!";
        assert SystemDefs.JavabaseBM.getNumUnpinnedBuffers() == SystemDefs.JavabaseBM.getNumBuffers() : "*** The heap-file scan has left pinned pages";

        System.out.println ("  Test 2 completed successfully.\n");
        return true;
    }

    protected boolean test3 () {

        System.out.println ("\n  Test 3: Update fixed-size records\n");
        Stream stream = null;
        MID rid = new MID();
        SmallMapFile f = null;

        System.out.println ("  - Open the same heap file as tests 1 and 2\n");
        try {
            f = new SmallMapFile("file_1", "row1", 1);
        } catch (Exception e) {
            System.err.println ("*** Could not create heap file\n");
            e.printStackTrace();
            return false;
        }

        System.out.println ("  - Change the records\n");
        Map map = null;
        try {
            stream = f.openStream();
            map = stream.getNext(rid);
        }
        catch (Exception e) {
            System.err.println ("*** Error opening scan\n");
            e.printStackTrace();
            return false;
        }


        int count = 0;
        while (map != null) {
            try {
                count += 2;

                Integer newVal = Integer.parseInt(map.getValue()) * 10;
                map.setValue(newVal.toString());
                f.updateMap(rid, map);

                map = stream.getNext(rid);
            } catch (Exception e) {
                e.printStackTrace();
                stream.closestream();
                return false;
            }
        }

        stream.closestream();
        assert count == 100 : "*** Record count before updating does not match!!! Found " + count / 2 + " records!";
        assert SystemDefs.JavabaseBM.getNumUnpinnedBuffers() == SystemDefs.JavabaseBM.getNumBuffers() : "*** The heap-file scan has left pinned pages";


        System.out.println ("  - Check that the updates are really there\n");
        try {
            stream = f.openStream();
            map = stream.getNext(rid);
        }
        catch (Exception e) {
            System.err.println ("*** Error opening scan\n");
            e.printStackTrace();
            return false;
        }

        count = 0;
        while (map != null) {
            try {
                count += 2;

                assert map.getRowLabel().equals("row1") : "Got row label " + map.getRowLabel() + " but expected row1";
                assert map.getColumnLabel().equals("col" + randoms[count]) : "Got row label " + map.getColumnLabel() + " but expected col" + randoms[count];
                assert map.getTimeStamp() == randoms[count] : "Got row label " + map.getTimeStamp() + " but expected " + randoms[count];
                assert Integer.parseInt(map.getValue()) == randoms[count] * 10 : "Got value " + map.getValue() + " but expected " + randoms[count];

                map = stream.getNext(rid);
            } catch (Exception e) {
                e.printStackTrace();
                stream.closestream();
                return false;
            }
        }

        stream.closestream();

        try {
            f.test();
        } catch (Exception e) {
            e.printStackTrace();
            return false;
        }

        assert count == 100 : "*** Record count after updating does not match!!! Found " + count / 2 + " records!";
        assert SystemDefs.JavabaseBM.getNumUnpinnedBuffers() == SystemDefs.JavabaseBM.getNumBuffers() : "*** The heap-file scan has left pinned pages";

        System.out.println ("  Test 3 completed successfully.\n");
        return true;
    }
}

public class SmallMapFileTest {
    public static void main(String[] args) {
        SmallMapFileTestDriver fs = new SmallMapFileTestDriver();
        boolean status = false;
        try {
            status = fs.runTests();
        } catch (IOException e) {
            e.printStackTrace();
        }

        if (!status) {
            System.out.println("Error ocurred during test");
            Runtime.getRuntime().exit(1);
        }

        System.out.println("test completed successfully");
        Runtime.getRuntime().exit(0);
    }
}
