package tests;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;
import java.util.stream.Collectors;

import BigT.Map;
import diskmgr.Page;
import global.GlobalConst;
import global.PageId;
import global.RID;
import global.MID;
import global.SystemDefs;
import heap.HFException;
import storage.SmallMap;
import storage.SmallMapPage;
import storage.SmallPageIterator;

class SmallMapPageTestDriver extends TestDriver implements GlobalConst {

    public SmallMapPageTestDriver() {
        super("Small Map Page Test");
    }

    protected String testName() {
        return "Small Map Page";
    }

    protected boolean test1() {
        System.out.println ("\n  Test 1: Insert and iterate over page maps\n");
        try {

            System.out.println ("  - Create new page and pin it to Buffer\n");
            PageId newPageId = new PageId();

            Page apage = new Page();
            PageId pageId = new PageId();
            pageId = SystemDefs.JavabaseBM.newPage(apage, 1);

            if (pageId == null)
                return false;

            newPageId.pid = pageId.pid;
            SmallMapPage newPage = new SmallMapPage(GlobalConst.MAXROWLABELSIZE);
            newPage.init(newPageId, apage, GlobalConst.MAXROWLABELSIZE, "row1");

            newPage.setCurPage(newPageId);
            newPage.setPrevPage(new PageId(-1));
            newPage.setNextPage(new PageId(-1));

            System.out.println ("  - Insert records into page\n");
            RID rid = new RID();
            Random rand = new Random();
            ArrayList<Integer> randoms = new ArrayList<Integer>();
            while (rid != null) {
                int in = rand.nextInt(1000);
                try {
                    SmallMap map = new SmallMap();
                    map.setValue(Integer.toString(in));
                    map.setTimeStamp(in);
                    map.setLabel("col" + in);
                    rid = newPage.insertRecord(map.getMapByteArray());

                    // Because we cant insert more than numRecsInPage records in a page
                    if (rid != null)
                        randoms.add(in);
                } catch (Exception e) {
                    e.printStackTrace();
                    return false;
                }
            }

            SystemDefs.JavabaseBM.unpinPage(pageId, true);
            // The real test begins

            System.out.println ("  - Iterate over page\n");
            SmallPageIterator itr = new SmallPageIterator(pageId, 1, MAXROWLABELSIZE, 3);
            MID mid = new MID();

            Map res;
            for (Integer integer : randoms.stream().sorted().collect(Collectors.toList())) {
                res = itr.getNext(mid);

                assert res.getRowLabel().equals("row1")
                        : "Expected Row row1 got " + res.getRowLabel();
                assert res.getColumnLabel().equals("col" + integer)
                        : "Expected Column " + "col" + integer + " got " + res.getColumnLabel();
                assert res.getTimeStamp() == integer
                        : "Expected Timestamp " + integer + " got " + res.getTimeStamp();
                assert Integer.parseInt(res.getValue()) == integer
                        : "Expected Value " + integer + " got " + res.getValue();
            }

            itr.closestream();
            assert SystemDefs.JavabaseBM.getNumUnpinnedBuffers() == SystemDefs.JavabaseBM.getNumBuffers()
                    : "*** The heap-file scan has left pinned pages "
                        + SystemDefs.JavabaseBM.getNumUnpinnedBuffers()
                        + "/"
                        + SystemDefs.JavabaseBM.getNumBuffers();

            // Test ends
            System.out.println ("  - Free page\n");
            SystemDefs.JavabaseBM.freePage(pageId);

        } catch (Exception e) {
            e.printStackTrace();
            return false;
        }

        System.out.println ("  Test 1 completed successfully.\n");
        return true;
    }


    protected boolean test2() {
        System.out.println ("\n  Test 2: Insert and sort within a page\n");

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



        System.out.println ("  Test 2 completed successfully.\n");
        return true;
    }
}

public class SmallMapPageTest {
    public static void main(String argv[]) {
        SmallMapPageTestDriver fs = new SmallMapPageTestDriver();
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
