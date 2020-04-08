package tests;

import java.io.IOException;

import BigT.Map;
import global.GlobalConst;
import storage.SmallMap;

class SmallMapTestDriver extends TestDriver implements GlobalConst {

    public SmallMapTestDriver() {
        super("Small Map Test");
    }

    protected String testName() {
        return "Small Map";
    }

    protected boolean test1() {
        SmallMap smallMap;
        Map map;

        try {
            smallMap = new SmallMap();
            smallMap.setLabel("label1");
            smallMap.setTimeStamp(100);
            smallMap.setValue("1");

            smallMap.print();

            map = new Map();
            map.setRowLabel("row1");
            map.setColumnLabel("col1");
            map.setTimeStamp(1);
            map.setValue("1");

            map.print();

            smallMap = new SmallMap(map, 1);
            smallMap.print();

            smallMap = new SmallMap(map, 2);
            smallMap.print();

            SmallMap testMap = new SmallMap(smallMap.getMapByteArray(), 0);
            testMap.print();

        } catch (Exception e) {
            e.printStackTrace();
            return false;
        }

        return true;
    }
}

public class SmallMapTest {
    public static void main(String argv[]) {
        SmallMapTestDriver fs = new SmallMapTestDriver();
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
