package tests;

import BigT.Map;
import BigT.RowJoin;
import BigT.Stream;
import BigT.bigT;
import driver.BatchInsert;
import global.GlobalConst;
import global.MID;
import global.SystemDefs;
import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVParser;
import org.apache.commons.csv.CSVRecord;
import org.apache.commons.io.input.BOMInputStream;

import java.io.File;
import java.io.InputStreamReader;
import java.io.Reader;

class RowJoinTestDriver extends TestDriver implements GlobalConst {
    private final static boolean OK = true;
    private final static boolean FAIL = false;

    private int choice = 100;
    private final static int reclen = 32;

    public RowJoinTestDriver() {
        super("bigttest");
    }

    public boolean runTests() {
        System.out.println("\n" + "Running " + testName() + " tests...." + "\n");

        SystemDefs sysdef = new SystemDefs(dbpath, 10000, 1000, "Clock");

        // Kill anything that might be hanging around
        String newdbpath;
        String newlogpath;

        newdbpath = dbpath;
        newlogpath = logpath;

        // Commands here is very machine dependent.  We assume
        // user are on UNIX system here
        new File(newlogpath).delete();
        new File(newdbpath).delete();


        new File(newlogpath).delete();
        new File(newdbpath).delete();

        //Run the tests. Return type different from C++
        boolean _pass = runTest2();
        if(_pass) {
            _pass = runTest1();
        }

        //Clean up again
        new File(newlogpath).delete();
        new File(newdbpath).delete();

        System.out.print("\n" + "..." + testName() + " tests ");
        System.out.print(_pass == OK ? "completely successfully" : "failed");
        System.out.print(".\n\n");

        return _pass;
    }

    /*
     * Test 1 focuses on the Map class and ensuring all methods are functioning as expected
     */
    public static boolean runTest1() {
        try {
            System.out.println("---------------------------------");
            System.out.println("Starting test 1 - RowJoin.java");
            System.out.println("---------------------------------");

            System.out.println("Creating maps");

            bigT b1 = new bigT("test_rj1", 1);
            bigT b2 = new bigT("test_rj2", 1);
            String columnFilter = "C1";

            Reader reader = new InputStreamReader(new BOMInputStream(BatchInsert.class.getResourceAsStream("/data/rowJoinTest.csv")), "UTF-8");
            CSVParser csvParser = new CSVParser(reader, CSVFormat.DEFAULT);
            int count = 0;
            Map tempMap;
            for (CSVRecord csvRecord : csvParser) {
                if(count<9) {
                    tempMap = new Map();
                    tempMap.setRowLabel(csvRecord.get(0));
                    tempMap.setColumnLabel(csvRecord.get(1));
                    tempMap.setTimeStamp(Integer.parseInt(csvRecord.get(3)));
                    tempMap.setValue(csvRecord.get(2));
                    b1.insertMap(tempMap.getMapByteArray());
                    count++;
                }
                else if(count >= 9) {
                    tempMap = new Map();
                    tempMap.setRowLabel(csvRecord.get(0));
                    tempMap.setColumnLabel(csvRecord.get(1));
                    tempMap.setTimeStamp(Integer.parseInt(csvRecord.get(3)));
                    tempMap.setValue(csvRecord.get(2));
                    b2.insertMap(tempMap.getMapByteArray());
                    count++;
                }
                if (count >= 100){
                    break;
                }
            }
            System.out.println("Buffers before Operation : "+SystemDefs.JavabaseBM.getNumUnpinnedBuffers());


            bigT outB = RowJoin.rowJoin(b1, b2, "rowJoinOut", columnFilter ,200);

            //bigT outB = RowSort.rowSort(b1,"Mule",100);
            Stream stream = new Stream(outB.getHf());
            Map stMap;
            MID m = new MID();
            while ((stMap=stream.getNext(m))!=null){
                stMap.print();
            }
            stream.closestream();
            System.out.println("Buffers after Operation : "+SystemDefs.JavabaseBM.getNumUnpinnedBuffers());
            System.out.println("\n------------------------------------------");
            System.out.println("Testing Done - RowJoin.java");
            System.out.println("------------------------------------------");
        } catch (Exception e){
            e.printStackTrace();
            return false;
        }
        return true;
    }

    public static boolean runTest2() {
        try {
            System.out.println("---------------------------------");
            System.out.println("Starting test 2 - RowJoin.java");
            System.out.println("---------------------------------");

            System.out.println("Creating maps");

            bigT b1 = new bigT("test_rj12", 1);
            bigT b2 = new bigT("test_rj22", 1);
            String columnFilter = "Mule";

            Reader reader = new InputStreamReader(new BOMInputStream(BatchInsert.class.getResourceAsStream("/data/project2_testdata.csv")), "UTF-8");
            CSVParser csvParser = new CSVParser(reader, CSVFormat.DEFAULT);
            int count = 0;
            Map tempMap;
            for (CSVRecord csvRecord : csvParser) {
                if(count<1000) {
                    tempMap = new Map();
                    tempMap.setRowLabel(csvRecord.get(0));
                    tempMap.setColumnLabel(csvRecord.get(1));
                    tempMap.setTimeStamp(Integer.parseInt(csvRecord.get(3)));
                    tempMap.setValue(csvRecord.get(2));
                    b1.insertMap(tempMap.getMapByteArray());
                    count++;
                }
                else if(count >= 1000) {
                    tempMap = new Map();
                    tempMap.setRowLabel(csvRecord.get(0));
                    tempMap.setColumnLabel(csvRecord.get(1));
                    tempMap.setTimeStamp(Integer.parseInt(csvRecord.get(3)));
                    tempMap.setValue(csvRecord.get(2));
                    b2.insertMap(tempMap.getMapByteArray());
                    count++;
                }
                if (count >= 2000){
                    break;
                }
            }
            System.out.println("Buffers before Operation : "+SystemDefs.JavabaseBM.getNumUnpinnedBuffers());


            bigT outB2 = RowJoin.rowJoin(b1, b2, "rowJoinOut2", columnFilter ,200);

            //bigT outB = RowSort.rowSort(b1,"Mule",100);
            Stream stream = new Stream(outB2.getHf());
            Map stMap;
            MID m = new MID();
            while ((stMap=stream.getNext(m))!=null){
                stMap.print();
            }
            stream.closestream();
            System.out.println("Buffers after Operation : "+SystemDefs.JavabaseBM.getNumUnpinnedBuffers());
            System.out.println("\n------------------------------------------");
            System.out.println("Testing Done - RowJoin.java");
            System.out.println("------------------------------------------");
        } catch (Exception e){
            e.printStackTrace();
            return false;
        }
        return true;
    }
}

public class RowJoinTest {
    public static void main(String[] args) {
        new RowJoinTestDriver().runTests();
    }
}