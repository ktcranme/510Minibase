package tests;

import java.io.IOException;
import java.util.*;
import java.util.stream.Collectors;

import BigT.Map;
import diskmgr.Page;
import global.*;
import storage.SmallMap;
import storage.SmallMapPage;
import storage.Stream;
import storage.SmallMapFile;

class SmallMapFileTestDriver extends TestDriver implements GlobalConst {
    int numRec = 500;

    public void setNumRec(Integer numRec) {
        this.numRec = numRec;
        randoms = new Integer[numRec];
    }

    public Integer[] randoms = new Integer[numRec];

    public SmallMapFileTestDriver() {
        super("Small Map File Test");
    }

    protected String testName() {
        return "Small Map File";
    }

    protected boolean test5() {
        System.out.println ("\n  Test 5: Insert and sort within a page\n");

        List<Integer> randoms = new ArrayList<>();
        Random rand = new Random();
        SmallMapPage page = new SmallMapPage(MAXROWLABELSIZE);
        try {
            System.out.println ("  - Create a page\n");
            page.init(new PageId(1), new Page(), MAXROWLABELSIZE, "row1");
        } catch (IOException e) {
            e.printStackTrace();
            return false;
        }

        System.out.println ("  - Insert records into page\n");
        RID rid = new RID();
        while (rid != null) {
            int in = rand.nextInt(1000);
            try {
                SmallMap map = new SmallMap();
                map.setValue(Integer.toString(in));
                map.setTimeStamp(in);
                map.setLabel("col" + in);
                rid = page.insertRecord(map.getMapByteArray());

                // Because we cant insert more than numRecsInPage records in a page
                if (rid != null)
                    randoms.add(in);
//                map.print();
            } catch (Exception e) {
                e.printStackTrace();
                return false;
            }
        }

        System.out.println ("  - Verify the sorted order\n");
        List<Integer> sorted = randoms.stream().sorted().collect(Collectors.toList());

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

            assert count == randoms.size() : "Count of records did not match insert count!";
        } catch (Exception e) {
            e.printStackTrace();
            return false;
        }

        System.out.println ("  Test 5 completed successfully.\n");
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

        List<Integer> sorted;
        if (numRec != 0) {
            sorted = Arrays.asList(randoms).stream().sorted().collect(Collectors.toList());
        } else {
            sorted = new ArrayList<>();
        }

        Map map = new Map();
        int count = 0;
        while (map != null) {
            try {
                if (numRec > 0)
                    assert SystemDefs.JavabaseBM.getNumUnpinnedBuffers() != SystemDefs.JavabaseBM.getNumBuffers()
                            : "*** The heap-file scan has not pinned any pages";
                else
                    assert SystemDefs.JavabaseBM.getNumUnpinnedBuffers() == SystemDefs.JavabaseBM.getNumBuffers()
                            : "*** The heap-file scan has left pages pinned despite having 0 records";

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
                String row = "row" + Integer.toString(randoms[i] % 50 * 2);
                m1.setRowLabel(row);
                m1.setColumnLabel("col" + randoms[i]);
                m1.setTimeStamp(randoms[i]);
                m1.setValue(Integer.toString(randoms[i]));

                if (!groups.containsKey(row)) {
                    groups.put(row, new ArrayList<>());
                }
                groups.get(row).add(randoms[i]);

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
                    for (Integer integer : groupSorted) {
                        assert map.getRowLabel().equals(primary)
                                : "Expected Row label " + primary + ", got " + map.getRowLabel();
                        assert Integer.parseInt(map.getValue()) == integer
                                : "Expected value " + integer + ", got value " + Integer.parseInt(map.getValue());

                        map = stream.getNext(rid);
                        count++;
                    }
                    groups.remove(primary);
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

        assert SystemDefs.JavabaseBM.getNumUnpinnedBuffers() == SystemDefs.JavabaseBM.getNumBuffers() : "*** The heap-file scan has left pinned pages";

        System.out.println ("  Test 3 completed successfully.\n");
        return true;
    }

    protected boolean test4 () {
        System.out.println ("\n  Test 4: Delete all records\n");
        Stream stream = null;
        MID rid = new MID();
        SmallMapFile f = null;

        System.out.println ("  - Open the same heap file as tests 1 and 2\n");
        try {
            f = new SmallMapFile("file_1", 1, 3, MAXROWLABELSIZE);
        } catch (Exception e) {
            System.err.println ("*** Could not create heap file\n");
            e.printStackTrace();
            return false;
        }

        Map map = null;
        System.out.println ("  - Delete all the records\n");
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
                f.deleteMap(rid);
                map = stream.getNext(rid);
                count++;
            } catch (Exception e) {
                e.printStackTrace();
                return false;
            }
        }

        System.out.println ("  - Verify record count\n");
        assert count == numRec
                : "Iterated over " + count + " records, but expected " + numRec + " records";

        try {
            assert f.getMapCnt() == 0 :
                    "Expected 0 records in file, got " + f.getMapCnt() + " records remaining";
        } catch (Exception e) {
            e.printStackTrace();
            return false;
        }

        try {
            stream.closestream();
        } catch (Exception e) {
            e.printStackTrace();
            return false;
        }
        assert SystemDefs.JavabaseBM.getNumUnpinnedBuffers() == SystemDefs.JavabaseBM.getNumBuffers() : "*** The heap-file scan has left pinned pages";
        return true;
    }

    protected boolean test6 () {
        System.out.println ("\n  Test 6: Re-Insert and scan fixed-size records\n");

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

        assert SystemDefs.JavabaseBM.getNumUnpinnedBuffers() == SystemDefs.JavabaseBM.getNumBuffers() : "*** The heap-file scan has left pinned pages " + SystemDefs.JavabaseBM.getNumUnpinnedBuffers() + "/" + SystemDefs.JavabaseBM.getNumBuffers();

        System.out.println ("  - Insert " + numRec + " random records\n");
        for (int i = 0; i < numRec; i++) {
            //fixed length record
            Map m1 = new Map();
            try {
                String row = "row" + Integer.toString(randoms[i] % 50 * 2);
                m1.setRowLabel(row);
                m1.setColumnLabel("col" + randoms[i]);
                m1.setTimeStamp(randoms[i]);
                m1.setValue(Integer.toString(randoms[i]));

                if (!groups.containsKey(row)) {
                    groups.put(row, new ArrayList<>());
                }
                groups.get(row).add(randoms[i]);

                f.insertMap(m1);

//                m1.print();
            } catch (Exception e) {
                System.err.println ("*** Could not make map");
                e.printStackTrace();
                return false;
            }
        }

        System.out.println ("  - Verify Sorted Stream\n");

        try {
            stream = f.openSortedStream();
        } catch (Exception e) {
            System.err.println ("*** Error opening scan\n");
            e.printStackTrace();
            return false;
        }

        List<Integer> sorted = Arrays.stream(randoms).sorted().collect(Collectors.toList());

        Map map = null;
        try {
            map = stream.getNext(rid);
        } catch (Exception e) {
            e.printStackTrace();
            return false;
        }

        int count = 0;
        while (map != null) {
            try {
                String primary = map.getRowLabel();
                List<Integer> groupSorted = groups.get(primary).stream().sorted().collect(Collectors.toList());
                for (Integer integer : groupSorted) {
                    assert map.getRowLabel().equals(primary)
                            : "Expected Row label " + primary + ", got " + map.getRowLabel();
                    assert Integer.parseInt(map.getValue()) == integer
                            : "Expected value " + integer + ", got value " + Integer.parseInt(map.getValue());

                    map = stream.getNext(rid);
                    count++;
                }
                groups.remove(primary);
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

        try {
            stream.closestream();
        } catch (Exception e) {
            e.printStackTrace();
            return false;
        }

        assert count == numRec : "Returned records from stream doesnt match insert count!";
        assert SystemDefs.JavabaseBM.getNumUnpinnedBuffers() == SystemDefs.JavabaseBM.getNumBuffers()
                : "*** The heap-file scan has left pinned pages";

        System.out.println ("  Test 6 completed successfully.\n");
        return true;
    }

    protected boolean test7() {
        SmallMapFile f;
        System.out.println ("  - Verify that DB.update_file_entry() actually renames the data file.\n");
        try {
            SystemDefs.JavabaseDB.update_file_entry("file_1", "file_new");
            f = new SmallMapFile("file_new", 1, 3, MAXROWLABELSIZE);
            assert f.getRecCnt() == numRec : "*** File reports " + f.getRecCnt() + " records, not " + numRec + " after rename";

            f = new SmallMapFile("file_1", 1, 3, MAXROWLABELSIZE);
            assert f.getRecCnt() == 0 : "*** File reports " + f.getRecCnt() + " records, not 0 after rename";

        } catch (Exception e) {
            e.printStackTrace();
            return false;
        }

        System.out.println ("  - Verify that file.deleteFile() actually deletes the entire file and frees all the pages.\n");
        try {
            f = new SmallMapFile("file_new", 1, 3, MAXROWLABELSIZE);
            f.deleteFile();
            assert SystemDefs.JavabaseBM.getNumUnpinnedBuffers() == SystemDefs.JavabaseBM.getNumBuffers()
                    : "*** The heap-file scan has left pinned pages";
            f = new SmallMapFile("file_new", 1, 3, MAXROWLABELSIZE);
            assert f.getRecCnt() == 0 : "*** File reports " + f.getRecCnt() + " records, not 0 after deletion";
        } catch (Exception e) {
            e.printStackTrace();
            return false;
        }

        return true;
    }
}

public class SmallMapFileTest {
    public static void main(String[] args) {
        SmallMapFileTestDriver fs = new SmallMapFileTestDriver();

        Integer[] lengths = {0, 50, 100, 179, 200, 29};
        boolean status = false;
        for (Integer length : lengths) {
            System.out.println("Running SmallMapFile tests with data size: " + length + "\n");
            try {
                fs.setNumRec(length);
                status = fs.runTests();
                if (!status)
                    break;
            } catch (IOException e) {
                System.out.println("Error occured running SmallMapFile tests with data size: " + length);
                e.printStackTrace();
                break;
            }
        }

        if (!status) {
            System.out.println("Error ocurred during test");
            Runtime.getRuntime().exit(1);
        }

        System.out.println("test completed successfully");
        Runtime.getRuntime().exit(0);
    }
}
