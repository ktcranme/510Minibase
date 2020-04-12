package tests;

import java.io.IOException;
import java.util.*;
import java.util.stream.Collectors;

import BigT.Map;
import bufmgr.*;
import diskmgr.Page;
import global.*;
import heap.HFBufMgrException;
import heap.InvalidSlotNumberException;
import heap.InvalidTupleSizeException;
import storage.SmallMap;
import storage.SmallMapPage;
import storage.Stream;
import storage.SmallMapFile;

class SmallMapFileTestDriver extends TestDriver implements GlobalConst {
    int numRec = 500;
    public Integer[] randoms = new Integer[numRec];

    public SmallMapFileTestDriver() {
        super("Small Map File Test");
    }

    protected String testName() {
        return "Small Map File";
    }

    protected boolean test4() {
        System.out.println ("\n  Test 4: Insert and sort within a page\n");
        Integer numRecsInPage = 25;

        Integer[] randoms = new Integer[numRecsInPage];
        Random rand = new Random();
        SmallMapPage page = new SmallMapPage(MAXROWLABELSIZE);
        try {
            System.out.println ("  - Create a page\n");
            page.init(new PageId(1), new Page(), MAXROWLABELSIZE, "row1");
        } catch (IOException e) {
            e.printStackTrace();
            return false;
        }

        System.out.println ("  - Insert " + numRecsInPage + " records\n");
        for (int i = 0; i < numRecsInPage; i++) {
            randoms[i] = rand.nextInt(1000);
            try {
                SmallMap map = new SmallMap();
                map.setValue(Integer.toString(randoms[i]));
                map.setTimeStamp(randoms[i]);
                map.setLabel("col" + randoms[i]);
                RID rid = page.insertRecord(map.getMapByteArray());

                // Because we cant insert more than numRecsInPage records in a page
                if (rid == null)
                    break;
//                map.print();
            } catch (Exception e) {
                e.printStackTrace();
                return false;
            }
        }

        System.out.println ("  - Verify the sorted order\n");
        List<Integer> sorted = Arrays.asList(randoms).stream().sorted().collect(Collectors.toList());

        try {
            page.sort(3);
            MID mid = page.firstSorted();
            int count = 0;

            while (mid != null) {
                Map map = page.getMap(mid, 1);
                assert Integer.parseInt(map.getValue()) == sorted.get(count)
                        : "Expected " + sorted.get(count) + ", got " + Integer.parseInt(map.getValue());
                assert map.getRowLabel().equals("row1");
                mid = page.nextSorted(mid);
                count++;
            }

            assert count == numRecsInPage : "Count of records did not match insert count!";
        } catch (Exception e) {
            e.printStackTrace();
            return false;
        }

        System.out.println ("  Test 4 completed successfully.\n");
        return true;
    }

    protected boolean test1() {
        System.out.println ("\n  Test 1: Insert and scan fixed-size records\n");
        Random rand = new Random();

        for (int i = 0; i < numRec; i++) {
            randoms[i] = rand.nextInt(5000);
        }

        MID rid = new MID();
        SmallMapFile f = null;

        System.out.println ("  - Create a heap file\n");
        try {
            f = new SmallMapFile("file_1", 1, 3, MAXROWLABELSIZE);
        } catch (Exception e) {
            System.err.println ("*** Could not create heap file\n");
            e.printStackTrace();
            return false;
        }

        assert SystemDefs.JavabaseBM.getNumUnpinnedBuffers() == SystemDefs.JavabaseBM.getNumBuffers() : "*** The heap-file scan has left pinned pages " + SystemDefs.JavabaseBM.getNumUnpinnedBuffers() + "/" + SystemDefs.JavabaseBM.getNumBuffers();

        System.out.println ("  - Insert " + numRec + " random records\n");
        for (int i = 0; i < numRec; i++) {
            //fixed length record
            Map m1 = new Map();
            try {
                m1.setRowLabel("row1");
                m1.setColumnLabel("col" + randoms[i]);
                m1.setTimeStamp(randoms[i]);
                m1.setValue(Integer.toString(randoms[i]));

                f.insertMap(m1);

//                m1.print();
            } catch (Exception e) {
                System.err.println ("*** Could not make map");
                e.printStackTrace();
                return false;
            }
        }

        Stream stream = null;
        System.out.println ("  - Verify Sorted Stream\n");

        try {
            stream = f.openSortedStream();
        } catch (Exception e) {
            System.err.println ("*** Error opening scan\n");
            e.printStackTrace();
            return false;
        }

        List<Integer> sorted = Arrays.asList(randoms).stream().sorted().collect(Collectors.toList());

        Map map = new Map();
        int count = 0;
        while (map != null) {
            try {
                assert SystemDefs.JavabaseBM.getNumUnpinnedBuffers() != SystemDefs.JavabaseBM.getNumBuffers() : "*** The heap-file scan has not pinned any pages";
                map = stream.getNext(rid);
                if (map == null)
                    break;
//                map.print();

                assert map.getRowLabel().equals("row1");
                assert Integer.parseInt(map.getValue()) == sorted.get(count) : "Expected value " + sorted.get(count) + ", got " + Integer.parseInt(map.getValue());
                count++;
            } catch (Exception e) {
                e.printStackTrace();
                return false;
            }
        }

        try {
            assert f.getRecCnt() == numRec : "*** File reports " + f.getRecCnt() + " records, not " + numRec;
        } catch (Exception e) {
            System.err.println ("*** Could not invoke getRecCnt on file\n");
            e.printStackTrace();
            return false;
        }

        assert count == numRec : "Returned records from stream doesnt match insert count!";
        assert SystemDefs.JavabaseBM.getNumUnpinnedBuffers() == SystemDefs.JavabaseBM.getNumBuffers() : "*** The heap-file scan has left pinned pages";

        System.out.println ("  Test 1 completed successfully.\n");
        return true;
    }

    protected boolean test2 () {
        System.out.println ("\n  Test 2: Delete fixed-size records\n");
        Stream stream = null;
        MID rid = new MID();
        SmallMapFile f = null;

        System.out.println ("  - Open the same heap file as test 1\n");
        try {
            f = new SmallMapFile("file_1", 1, 3, MAXROWLABELSIZE);
        } catch (Exception e) {
            System.err.println ("*** Could not create heap file\n");
            e.printStackTrace();
            return false;
        }

        Map map = null;
        System.out.println ("  - Delete half the records\n");
        try {
            stream = f.openSortedStream();
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

                // Basically deleting all even records :D
                if (count % 2 != 0) {
                    f.deleteMap(rid);
                }

                map = stream.getNext(rid);
            } catch (Exception e) {
                e.printStackTrace();
                return false;
            }
        }

        try {
            stream.closestream();
        } catch (Exception e) {
            e.printStackTrace();
            return false;
        }

        assert count == numRec : "*** Record count before deletion does not match!!! Found " + count + " records!";
        assert SystemDefs.JavabaseBM.getNumUnpinnedBuffers() == SystemDefs.JavabaseBM.getNumBuffers() : "*** The heap-file scan has left pinned pages";

        System.out.println ("  - Scan the remaining records\n");
        try {
            stream = f.openSortedStream();
            map = stream.getNext(rid);
        }
        catch (Exception e) {
            System.err.println ("*** Error opening scan\n");
            e.printStackTrace();
            return false;
        }

        List<Integer> sorted = Arrays.asList(randoms).stream().sorted().collect(Collectors.toList());

        count = 1;
        int numRecordsAfterDel = 0;
        while (map != null) {
            try {

                assert map.getRowLabel().equals("row1")
                        : "Got row label " + map.getRowLabel() + " but expected row1";
                assert map.getColumnLabel().equals("col" + sorted.get(count))
                        : "Got row label " + map.getColumnLabel() + " but expected col" + sorted.get(count);
                assert map.getTimeStamp() == sorted.get(count)
                        : "Got row label " + map.getTimeStamp() + " but expected " + sorted.get(count);
                assert Integer.parseInt(map.getValue()) == sorted.get(count)
                        : "Got value " + map.getValue() + " but expected " + sorted.get(count);
//                map.print();
                count += 2;
                numRecordsAfterDel++;
                map = stream.getNext(rid);
            } catch (Exception e) {
                e.printStackTrace();
                return false;
            }
        }

        try {
            stream.closestream();
        } catch (Exception e) {
            e.printStackTrace();
            return false;
        }

        assert numRecordsAfterDel == numRec / 2 : "*** Record count before deletion does not match!!! Iterated over " + numRecordsAfterDel  + " records!";
        try {
            assert f.getMapCnt() == numRec / 2 : "*** Record count before deletion does not match!!! File reports " + f.getMapCnt()  + " records!";
        } catch (Exception e) {
            e.printStackTrace();
            return false;
        }
        assert SystemDefs.JavabaseBM.getNumUnpinnedBuffers() == SystemDefs.JavabaseBM.getNumBuffers() : "*** The heap-file scan has left pinned pages";

        System.out.println ("  Test 2 completed successfully.\n");
        return true;
    }

    protected boolean test3 () {

        System.out.println ("\n  Test 3: Insert and read multiple primary records\n");
        Stream stream = null;
        MID rid = new MID();
        SmallMapFile f = null;
        HashMap<String, List<Integer>> groups = new HashMap<>();

        System.out.println ("  - Open the same heap file as tests 1 and 2\n");
        try {
            f = new SmallMapFile("file_1", 1, 3, MAXROWLABELSIZE);
        } catch (Exception e) {
            System.err.println ("*** Could not create heap file\n");
            e.printStackTrace();
            return false;
        }

        for (int i = 0; i < numRec; i += 2) {
            //fixed length record
            Map m1 = new Map();
            try {
                // Create 9 different primaries (row1 already exists)
                m1.setRowLabel("row" + (randoms[i] % 8 * 2));
                m1.setColumnLabel("col" + randoms[i]);
                m1.setTimeStamp(randoms[i]);
                m1.setValue(Integer.toString(randoms[i]));

                if (!groups.containsKey("row" + (randoms[i] % 8 * 2))) {
                    groups.put("row" + (randoms[i] % 8 * 2), new ArrayList<>());
                }
                groups.get("row" + (randoms[i] % 8 * 2)).add(randoms[i]);

                f.insertMap(m1);

//                m1.print();
            } catch (Exception e) {
                System.err.println ("*** Could not make map");
                e.printStackTrace();
                return false;
            }
        }

        try {
            assert f.getMapCnt() == numRec : "*** Record count after insertion does not match!!! File reports " + f.getMapCnt()  + " records!";
        } catch (Exception e) {
            e.printStackTrace();
            return false;
        }
        assert SystemDefs.JavabaseBM.getNumUnpinnedBuffers() == SystemDefs.JavabaseBM.getNumBuffers() : "*** The heap-file scan has left pinned pages";


        stream = null;
        System.out.println ("  - Verify Sorted Stream\n");

        try {
            stream = f.openSortedStream();
        } catch (Exception e) {
            System.err.println ("*** Error opening scan\n");
            e.printStackTrace();
            return false;
        }

        List<Integer> sorted = Arrays.stream(randoms).sorted().collect(Collectors.toList());

        Map map;
        try {
            map = stream.getNext(rid);
        } catch (Exception e) {
            e.printStackTrace();
            return false;
        }

        int count = 0;
        while (map != null) {
            try {
                assert SystemDefs.JavabaseBM.getNumUnpinnedBuffers() != SystemDefs.JavabaseBM.getNumBuffers() : "*** The heap-file scan has not pinned any pages";

                if (map.getRowLabel().equals("row1")) {
                    for (int i = 0; i < numRec / 2; i++) {
                        assert Integer.parseInt(map.getValue()) == sorted.get(i * 2 + 1) : "Expected value " + sorted.get(i * 2 + 1) + ", got " + Integer.parseInt(map.getValue());
                        map = stream.getNext(rid);
                        count++;
                    }
                } else {
                    String primary = map.getRowLabel();
                    List<Integer> groupSorted = groups.get(primary).stream().sorted().collect(Collectors.toList());
                    for (int i = 0; i < groupSorted.size(); i++) {
                        assert map.getRowLabel().equals(primary)
                                : "Expected Row label " + primary + ", got " + map.getRowLabel();
                        assert Integer.parseInt(map.getValue()) == groupSorted.get(i)
                                : "Expected value " + groupSorted.get(i) + ", got value " + Integer.parseInt(map.getValue());

                        map = stream.getNext(rid);
                        count++;
                    }
                }
            } catch (Exception e) {
                e.printStackTrace();
                return false;
            }
        }
        assert count == numRec : "*** Record count after insertion does not match!!! Iterated over " + count  + " records!";

        try {
            stream.closestream();
        } catch (Exception e) {
            e.printStackTrace();
            return false;
        }


        System.out.println ("  Test 3 completed successfully.\n");
        return true;
    }

//    protected boolean test1() {
//        System.out.println ("\n  Test 1: Insert and scan fixed-size records\n");
//        Random rand = new Random();
//        for (int i = 0; i < 100; i++) {
//            randoms[i] = rand.nextInt(1000);
//        }
//
//        MID rid = new MID();
//        SmallMapFile f = null;
//
//        System.out.println ("  - Create a heap file\n");
//        try {
//            f = new SmallMapFile("file_1", "row1", 1);
//        } catch (Exception e) {
//            System.err.println ("*** Could not create heap file\n");
//            e.printStackTrace();
//            return false;
//        }
//
//        assert SystemDefs.JavabaseBM.getNumUnpinnedBuffers() == SystemDefs.JavabaseBM.getNumBuffers() : "*** The heap-file scan has left pinned pages " + SystemDefs.JavabaseBM.getNumUnpinnedBuffers() + "/" + SystemDefs.JavabaseBM.getNumBuffers();
//
//        for (int i = 0; i < 100; i++) {
//            //fixed length record
//            Map m1 = new Map();
//            try {
//                m1.setRowLabel("row1");
//                m1.setColumnLabel("col" + randoms[i]);
//                m1.setTimeStamp(randoms[i]);
//                m1.setValue(Integer.toString(randoms[i]));
//
//                f.insertMap(m1);
//            } catch (Exception e) {
//                System.err.println ("*** Could not make map");
//                e.printStackTrace();
//                return false;
//            }
//        }
//
//        assert SystemDefs.JavabaseBM.getNumUnpinnedBuffers() == SystemDefs.JavabaseBM.getNumBuffers() : "*** The heap-file scan has left pinned pages " + SystemDefs.JavabaseBM.getNumUnpinnedBuffers() + "/" + SystemDefs.JavabaseBM.getNumBuffers();
//
//        try {
//            f.test();
//        } catch (HFBufMgrException e) {
//            e.printStackTrace();
//        } catch (IOException e) {
//            e.printStackTrace();
//        } catch (InvalidSlotNumberException e) {
//            e.printStackTrace();
//        } catch (InvalidTupleSizeException e) {
//            e.printStackTrace();
//        }
//
//        assert SystemDefs.JavabaseBM.getNumUnpinnedBuffers() == SystemDefs.JavabaseBM.getNumBuffers() : "*** The heap-file scan has left pinned pages " + SystemDefs.JavabaseBM.getNumUnpinnedBuffers() + "/" + SystemDefs.JavabaseBM.getNumBuffers();
//
//        return true;
//    }
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
