package tests;

import java.io.*;

import BigT.IndexScan;
import BigT.Stream;
import BigT.bigT;
import BigT.Map;

import BigT.*;

import java.lang.*;
import java.net.URL;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;

import btree.*;
import driver.BatchInsert;
import driver.FilterParser;
import global.*;
import heap.*;
import index.IndexException;
import index.UnknownIndexTypeException;
import iterator.CondExpr;
import iterator.FileScan;
import iterator.FldSpec;
import iterator.RelSpec;
import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVParser;
import org.apache.commons.csv.CSVRecord;
import org.apache.commons.io.input.BOMInputStream;
import storage.BigT;

import javax.sound.midi.MidiChannel;

import static BigT.bigT.INDEXFILENAMEPREFIX;
import static BigT.bigT.TSINDEXFILENAMEPREFIX;

class RowSortTestDriver extends TestDriver implements GlobalConst {
    private final static boolean OK = true;
    private final static boolean FAIL = false;

    private int choice = 100;
    private final static int reclen = 32;

    public RowSortTestDriver() {
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
        boolean _pass = runTest1();

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
            System.out.println("Starting test 1 - RowSort.java");
            System.out.println("---------------------------------");

            System.out.println("Creating maps");

            BigT b1 = new BigT("test_rs");

            Reader reader = new InputStreamReader(new BOMInputStream(BatchInsert.class.getResourceAsStream("/data/project2_testdata.csv")), "UTF-8");
            CSVParser csvParser = new CSVParser(reader, CSVFormat.DEFAULT);
            int count = 0;
            Map tempMap;
            for (CSVRecord csvRecord : csvParser) {
                if(count==5400)
                    break;
                count++;
                tempMap = new Map();
                tempMap.setRowLabel(csvRecord.get(0));
                tempMap.setColumnLabel(csvRecord.get(1));
                tempMap.setTimeStamp(Integer.parseInt(csvRecord.get(3)));
                tempMap.setValue(csvRecord.get(2));
                b1.insertMap(tempMap);
            }
            System.out.println("Buffers before Operation : "+SystemDefs.JavabaseBM.getNumUnpinnedBuffers());
            BigT outB = RowSort.rowSort(b1,"test123","Mule",100);
            MultiTypeFileStream stream = new MultiTypeFileStream(outB,null);
            Map stMap;
            while ((stMap=stream.get_next())!=null){
                stMap.print();
            }
            stream.close();
            System.out.println("Buffers after Operation : "+SystemDefs.JavabaseBM.getNumUnpinnedBuffers());
            System.out.println("\n------------------------------------------");
            System.out.println("Testing Done - RowSort.java");
            System.out.println("------------------------------------------");
        } catch (Exception e){
            e.printStackTrace();
            return false;
        }
        return true;
    }
}

public class RowSortTest {
    public static void main(String[] args) {
        new RowSortTestDriver().runTests();
    }
}