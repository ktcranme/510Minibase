package tests;

import java.io.*;

import BigT.IndexScan;
import BigT.Stream;
import BigT.bigT;
import BigT.Map;

import BigT.*;

import java.lang.*;
import java.net.URL;

import btree.*;
import driver.FilterParser;
import global.*;
import heap.*;
import index.IndexException;
import index.UnknownIndexTypeException;
import iterator.CondExpr;
import iterator.FileScan;
import iterator.FldSpec;
import iterator.RelSpec;

import javax.sound.midi.MidiChannel;

import static BigT.bigT.INDEXFILENAMEPREFIX;
import static BigT.bigT.TSINDEXFILENAMEPREFIX;

class bigTDriver extends TestDriver implements GlobalConst{
    private final static boolean OK = true;
    private final static boolean FAIL = false;

    private int choice = 100;
    private final static int reclen = 32;

    public bigTDriver () {
        super("bigttest");
    }
    public boolean runTests(){
        System.out.println ("\n" + "Running " + testName() + " tests...." + "\n");

        SystemDefs sysdef = new SystemDefs(dbpath,1000,100,"Clock");

        // Kill anything that might be hanging around
        String newdbpath;
        String newlogpath;
        String remove_logcmd;
        String remove_dbcmd;
        String remove_cmd = "/bin/rm -rf ";

        newdbpath = dbpath;
        newlogpath = logpath;

        remove_logcmd = remove_cmd + logpath;
        remove_dbcmd = remove_cmd + dbpath;

        // Commands here is very machine dependent.  We assume
        // user are on UNIX system here
        try {
            Runtime.getRuntime().exec(remove_logcmd);
            Runtime.getRuntime().exec(remove_dbcmd);
        }
        catch (IOException e) {
            System.err.println ("IO error: "+e);
        }

        remove_logcmd = remove_cmd + newlogpath;
        remove_dbcmd = remove_cmd + newdbpath;

        try {
            Runtime.getRuntime().exec(remove_logcmd);
            Runtime.getRuntime().exec(remove_dbcmd);
        }
        catch (IOException e) {
            System.err.println ("IO error: "+e);
        }

        //Run the tests. Return type different from C++
        boolean _pass = runTest1();
        boolean _pass2 = runTest3();

        //Clean up again
        try {
            Runtime.getRuntime().exec(remove_logcmd);
            Runtime.getRuntime().exec(remove_dbcmd);
        }
        catch (IOException e) {
            System.err.println ("IO error: "+e);
        }

        System.out.print ("\n" + "..." + testName() + " tests ");
        System.out.print (_pass==OK && _pass2==OK ? "completely successfully" : "failed");
        System.out.print (".\n\n");

        return _pass;
    }

    /*
     * Test 1 focuses on the Map class and ensuring all methods are functioning as expected
     */
    public static boolean runTest1() {
        System.out.println("---------------------------------");
        System.out.println("Starting test 1 - bigT.java");
        System.out.println("---------------------------------");

        System.out.println("Creating maps");
        //create map and print it out
        Map m = new Map();
        Map m7 = new Map();
        Map m8 = new Map();
        Map m9 = new Map();
        try {
            m.setRowLabel("Dominica");
            m.setColumnLabel("Zebra");
            m.setTimeStamp(46067);
            m.setValue("3");

            m7.setRowLabel("Dominica");
            m7.setColumnLabel("Zebra");
            m7.setTimeStamp(46021);
            m7.setValue("2");

            m8.setRowLabel("Dominica");
            m8.setColumnLabel("Zebra");
            m8.setTimeStamp(460);
            m8.setValue("2");

            m9.setRowLabel("Dominica");
            m9.setColumnLabel("Zebra");
            m9.setTimeStamp(46);
            m9.setValue("1");
        } catch (IOException e) {
            System.out.println("IOException while creating a map");
            e.printStackTrace();
        }
        Map m2 = new Map(m.getMapByteArray(), 0);
        try {
            m2.setColumnLabel("Giraffe");
        } catch (IOException e) {
            System.out.println("IOException while creating a map from a bytearray");
            e.printStackTrace();
        }
        Map m3 = new Map(m);
        try {
            m3.setTimeStamp(51073);
        } catch (IOException e) {
            System.out.println("IOException while creating a map with another map");
            e.printStackTrace();
        }
        Map m4 = new Map();
        Map m5 = new Map();
        Map m6 = new Map(m2);
        m4.mapCopy(m);
        m5.mapInit(m2.getMapByteArray(), 0);
        m6.mapSet(m3.getMapByteArray(), 0);

        try {
            m.print();
            m2.print();
            m3.print();
            m4.print();
            m5.print();
            m6.print();
            m7.print();
            m8.print();
            m9.print();
        } catch (IOException e) {
            System.out.println("IOException while testing the constructor replacement methods");
            e.printStackTrace();
        }

        // Test BigT functionalities
        int mapCnt = -1, rowCnt = -1, colCnt = -1;
        try {
            System.out.println("Creating a Big Table  with indexing as 1");
            bigT big = new bigT("Testing1", 1);
            big.insertMap(m.getMapByteArray());
            big.insertMap(m2.getMapByteArray());
            big.insertMap(m3.getMapByteArray());
            big.insertMap(m4.getMapByteArray());
            big.insertMap(m5.getMapByteArray());
            big.insertMap(m6.getMapByteArray());
            big.insertMap(m7.getMapByteArray());
            big.insertMap(m8.getMapByteArray());
            big.insertMap(m9.getMapByteArray());
            mapCnt = big.getMapCnt();
            System.out.println("Big1 map count:" + mapCnt);
            rowCnt = big.getRowCnt();
            System.out.println("Big1 row count:" + rowCnt);
            colCnt = big.getColumnCnt();
            System.out.println("Big1 column count:" + colCnt);
            System.out.println("Deleting Big Table  with indexing as 1");
            Stream s = new Stream(big.getHf());
            MID mid = new MID();
            Map mk;
            while ((mk=s.getNext(mid))!=null){
                mk.print();
            }
            big.deleteBigt();

            System.out.println("Creating a Big Table  with indexing as 2");
            bigT big2 = new bigT("Testing_2", 2);
            big2.insertMap(m.getMapByteArray());
            big2.insertMap(m2.getMapByteArray());
            big2.insertMap(m3.getMapByteArray());
            big2.insertMap(m4.getMapByteArray());
            big2.insertMap(m5.getMapByteArray());
            big2.insertMap(m6.getMapByteArray());
            big2.insertMap(m7.getMapByteArray());
            big2.insertMap(m8.getMapByteArray());
            big2.insertMap(m9.getMapByteArray());
            mapCnt = big2.getMapCnt();
            System.out.println("Big2 map count:" + mapCnt);
            rowCnt = big2.getRowCnt();
            System.out.println("Big2 row count:" + rowCnt);
            colCnt = big2.getColumnCnt();
            System.out.println("Big2 column count:" + colCnt);
            CondExpr[] expr1 = FilterParser.parseSingle("1",4,AttrType.attrString);
            CondExpr[] expr2 = FilterParser.parseSingle("[A,Z]",1,AttrType.attrString);
            IndexScan is = new IndexScan(new IndexType(IndexType.B_Index),big2.getName(),INDEXFILENAMEPREFIX+big2.getName(),expr2,expr1,false);
            System.out.println("Index File created");
            Map tmpm;
            while((tmpm=is.get_next())!=null){
                tmpm.print();
            }
            is.close();

            big2.deleteBigt();
            System.out.println("Deleting Big Table  with indexing as 2");

            System.out.println("Creating a Big Table  with indexing as 3");
            bigT big3 = new bigT("Testing_3", 3);
            big3.insertMap(m.getMapByteArray());
            big3.insertMap(m2.getMapByteArray());
            big3.insertMap(m3.getMapByteArray());
            big3.insertMap(m4.getMapByteArray());
            big3.insertMap(m5.getMapByteArray());
            big3.insertMap(m6.getMapByteArray());
            big3.insertMap(m7.getMapByteArray());
            big3.insertMap(m8.getMapByteArray());
            big3.insertMap(m9.getMapByteArray());
            mapCnt = big3.getMapCnt();
            System.out.println("Big3 map count:" + mapCnt);
            rowCnt = big3.getRowCnt();
            System.out.println("Big3 row count:" + rowCnt);
            colCnt = big3.getColumnCnt();
            System.out.println("Big3 column count:" + colCnt);
            is = new IndexScan(new IndexType(IndexType.B_Index), big3.getName(), INDEXFILENAMEPREFIX + big3.getName(), null, null, false);
            System.out.println("Index File created");
            while ((tmpm = is.get_next()) != null) {
                tmpm.print();
            }
            is.close();
            big3.deleteBigt();
            System.out.println("Deleting Big Table  with indexing as 3");

            System.out.println("Creating a Big Table  with indexing as 4");
            bigT big4 = new bigT("Testing_4", 4);
            big4.insertMap(m.getMapByteArray());
            big4.insertMap(m2.getMapByteArray());
            big4.insertMap(m3.getMapByteArray());
            big4.insertMap(m4.getMapByteArray());
            big4.insertMap(m5.getMapByteArray());
            big4.insertMap(m6.getMapByteArray());
            big4.insertMap(m7.getMapByteArray());
            big4.insertMap(m8.getMapByteArray());
            big4.insertMap(m9.getMapByteArray());
            mapCnt = big4.getMapCnt();
            System.out.println("Big4 map count:" + mapCnt);
            rowCnt = big4.getRowCnt();
            System.out.println("Big4 row count:" + rowCnt);
            colCnt = big4.getColumnCnt();
            System.out.println("Big4 column count:" + colCnt);
            System.out.println("Deleting Big Table  with indexing as 4");
            is = new IndexScan(new IndexType(IndexType.B_Index), big4.getName(), INDEXFILENAMEPREFIX + big4.getName(), null, null, false);
            System.out.println("Index Scan");
            while ((tmpm = is.get_next()) != null) {
                tmpm.print();
            }
            is.close();

            is = new IndexScan(new IndexType(IndexType.B_Index), big4.getName(), TSINDEXFILENAMEPREFIX + big4.getName(), null, null, false);
            System.out.println("Index Scan TS");
            while ((tmpm = is.get_next()) != null) {
                tmpm.print();
            }
            is.close();
            big4.deleteBigt();

            System.out.println("Creating a Big Table  with indexing as 5");
            bigT big5 = new bigT("Testing_5", 5);
            big5.insertMap(m.getMapByteArray());
            big5.insertMap(m2.getMapByteArray());
            big5.insertMap(m3.getMapByteArray());
            big5.insertMap(m4.getMapByteArray());
            big5.insertMap(m5.getMapByteArray());
            big5.insertMap(m6.getMapByteArray());
            big5.insertMap(m7.getMapByteArray());
            big5.insertMap(m8.getMapByteArray());
            big5.insertMap(m9.getMapByteArray());
            mapCnt = big5.getMapCnt();
            System.out.println("Big5 map count:" + mapCnt);
            rowCnt = big5.getRowCnt();
            System.out.println("Big5 row count:" + rowCnt);
            colCnt = big5.getColumnCnt();
            System.out.println("Big5 column count:" + colCnt);

            System.out.println("Testing openStream for orderType 1 and index type 5");
            Iterator it = big5.openStream(1,"[A,E]","*","1");
            Map tt;
            while ((tt = it.get_next())!=null)
                tt.print();
            it.close();



            System.out.println("Testing openStream for orderType 5 and index type 5");
            it = big5.openStream(5,"[A,E]","*","1");
            while ((tt = it.get_next())!=null)
                tt.print();
            it.close();

            System.out.println("Test for index scan of maps!");
            is = new IndexScan(new IndexType(IndexType.B_Index), big5.getName(), INDEXFILENAMEPREFIX + big5.getName(), null, null, false);
            System.out.println("Index Scan");
            while ((tmpm = is.get_next()) != null) {
                tmpm.print();
            }
            is.close();

            is = new IndexScan(new IndexType(IndexType.B_Index), big5.getName(), TSINDEXFILENAMEPREFIX + big5.getName(), null, null, false);
            System.out.println("Index Scan TS");
            while ((tmpm = is.get_next()) != null) {
                tmpm.print();
            }
            is.close();
            big5.deleteBigt();
            System.out.println("Deleting Big Table with indexing as 5");
        } catch(Exception e){
            System.out.println(e);
            return false;
        }

        System.out.println("\n------------------------------------------");
        System.out.println("Finished Testing for BigT functionalities.");
        System.out.println("------------------------------------------");
        return true;
    }

    public static boolean runTest3(){
        System.out.println("---------------------------------");
        System.out.println("Starting test 2 - bigT.java");
        System.out.println("---------------------------------");
        try {
            bigT big = new bigT("Testing1", 1);
            Map temp;
            URL url = bigTDriver.class.getResource("/data/project2_testdata.csv");
            File csvFile = new File(url.getPath());

            BufferedReader br = new BufferedReader(new FileReader(csvFile));
            String line;
            String[] rec;
            int c = 0;
            while ((line = br.readLine()) != null) {
                rec = line.split(",");
                if(rec[0].length()>16 && rec[1].length()>16 && rec[3].length()>16)
                    continue;
                temp = new Map();
                if(c==0)
                    temp.setRowLabel(rec[0].substring(1));
                else
                    temp.setRowLabel(rec[0]);
                temp.setColumnLabel(rec[1]);
                temp.setTimeStamp(Integer.parseInt(rec[2]));
                temp.setValue(rec[3]);
                big.insertMap(temp.getMapByteArray());
                c++;
                if (c == 100)
                    break;
            }

            System.out.println("---------------------------------");
            System.out.println("Testing openStream for orderType 1 and index type 1");
            System.out.println("---------------------------------");
            Iterator it = big.openStream(1,"*","*","[1,8]");
            Map tt;
            while ((tt = it.get_next())!=null)
                tt.print();
            it.close();

            System.out.println("---------------------------------");
            System.out.println("Testing openStream for orderType 2 and index type 1");
            System.out.println("---------------------------------");
            it = big.openStream(2,"[A,E]","*","*");
            while ((tt = it.get_next())!=null)
                tt.print();
            it.close();

            System.out.println("---------------------------------");
            System.out.println("Testing openStream for orderType 3 and index type 1");
            System.out.println("---------------------------------");
            it = big.openStream(3,"[A,E]","*","*");
            while ((tt = it.get_next())!=null)
                tt.print();
            it.close();

            System.out.println("---------------------------------");
            System.out.println("Testing openStream for orderType 4 and index type 1");
            System.out.println("---------------------------------");
            it = big.openStream(4,"[A,E]","*","*");
            while ((tt = it.get_next())!=null)
                tt.print();
            it.close();

            System.out.println("---------------------------------");
            System.out.println("Testing openStream for orderType 5 and index type 1");
            System.out.println("---------------------------------");
            it = big.openStream(5,"[A,E]","*","*");
            while ((tt = it.get_next())!=null)
                tt.print();
            it.close();

            big.deleteBigt();

            big = new bigT("Testing1", 5);

            br = new BufferedReader(new FileReader(csvFile));
            c = 0;
            while ((line = br.readLine()) != null) {
                rec = line.split(",");
                if(rec[0].length()>16 && rec[1].length()>16 && rec[3].length()>16)
                    continue;
                temp = new Map();
                if(c==0)
                    temp.setRowLabel(rec[0].substring(1));
                else
                    temp.setRowLabel(rec[0]);
                temp.setColumnLabel(rec[1]);
                temp.setTimeStamp(Integer.parseInt(rec[2]));
                temp.setValue(rec[3]);
                big.insertMap(temp.getMapByteArray());
                c++;
                if (c == 1000)
                    break;
            }
            System.out.println("---------------------------------");
            System.out.println("Testing openStream for orderType 5 and index type 5");
            System.out.println("---------------------------------");
            it = big.openStream(5,"*","*","*");
            while ((tt = it.get_next())!=null)
                tt.print();
            it.close();
        } catch (Exception e){
            System.out.println(e);
        }

        System.out.println("\n------------------------------------------");
        System.out.println("Finished Testing for openStream Scan.");
        System.out.println("------------------------------------------");
        return true;
    }
}

public class bigtTest {
    public static void main(String[] args) {
        new bigTDriver().runTests();
    }
}