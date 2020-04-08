package driver;

import BigT.Iterator;
import BigT.Map;
import BigT.bigT;
import bufmgr.PageNotReadException;
import diskmgr.BigDB;
import diskmgr.PCounter;
import global.GlobalConst;
import global.SystemDefs;
import heap.InvalidTypeException;
import index.IndexException;
import iterator.LowMemException;
import iterator.PredEvalException;
import iterator.SortException;
import iterator.UnknowAttrType;
import iterator.UnknownKeyTypeException;

import java.io.*;
import java.util.HashMap;
import java.util.regex.*;

import java.net.URL;
import java.util.ArrayList;
import java.util.List;

public class Driver {
    public static java.util.Map<String, BigDB> usedDbMap;
    public static int rprev_count, wprev_count, rnext_count, wnext_count, wcount, rcount, mapCnt = 0, rowCnt = 0,
            columnCnt = 0;
    public static long prev_time, next_time;
    public static boolean countsUpToDate = false;

    public static void main(String[] args) throws Exception {
        usedDbMap = new HashMap<>();
        String dbpath = "D:\\minibase_db\\" + "hf" + System.getProperty("user.name") + ".minibase-db";
        SystemDefs sysdef = new SystemDefs(dbpath, 100000, 500, "Clock");
        BufferedReader br = new BufferedReader(new InputStreamReader(System.in));
        System.out.println("Welcome to the BigTable interface");
        System.out.println("You have 6 options: BatchInsert and Query. Their structures follow:");
        printCommands();
        PCounter.initialize();
        // begin reading in all the commands
        String command = br.readLine();
        while (!command.toLowerCase().equals("quit") && !command.toLowerCase().equals("q")) {
            String[] tokens = command.trim().split("\\s++");

            // batchinsert
            if (tokens[0].toLowerCase().equals("batchinsert") && tokens.length == 5) {
                handleBatchInsert(tokens);
            }
            // query
            else if (tokens[0].toLowerCase().equals("query") && tokens.length == 8) {
                handleQuery(tokens);
            } 
            // get counts
            else if (tokens[0].toLowerCase().equals("getcounts") && tokens.length == 2) {
                handleGetCount(tokens);
            } 
            // mapinsert
            else if (tokens[0].toLowerCase().equals("mapinsert") && tokens.length == 8) {
                handleMapInsert(tokens);
            } 
            // rowjoin
            else if (tokens[0].toLowerCase().equals("rowjoin") && tokens.length == 6) {
                handleRowJoin(tokens);
            }
            // rowsort
            else if (tokens[0].toLowerCase().equals("rowsort") && tokens.length == 5) {
                handleRowSort(tokens);
            }
            // invalid command
            else {
                System.out.println(
                        "ERROR: The command you have entered does not match the corresponding number of parameters required.");
                System.out.println("The required structures are as follows:");
                printCommands();
            }

            System.out.println("Next command:\n");
            command = br.readLine();
        }
    }

    public static void handleBatchInsert(String[] tokens) throws Exception {
        String fileName = tokens[1];
        String typeStr = tokens[2];
        String bigtName = tokens[3];
        String numbufStr = tokens[4];

        // check that type is a single character and digit
        if (!isInteger(typeStr) || typeStr.length() != 1) {
            System.out.println("ERROR: TYPE must be a single digit 1 through 5");
        } else if (!isInteger(numbufStr)) {
            System.out.println("ERROR: NUMBUF must be an integer");
        } else {
            int type = Integer.parseInt(typeStr);
            int numbuf = Integer.parseInt(numbufStr);

            // check that data file exists
            URL url = Driver.class.getResource("/data/".concat(fileName));
            if (url == null) {
                System.out.println("ERROR: " + fileName + " does not exist in the /data/ folder.");
            }
            // check that the type is only values 1-5
            else if (type < 1 || type > 5) {
                System.out.println("ERROR: The type must be an integer between 1 and 5");
            } else {
                rprev_count = PCounter.rcounter;
                wprev_count = PCounter.wcounter;
                prev_time = System.currentTimeMillis();

                BatchInsert.batchinsert(fileName, type, bigtName, numbuf);

                next_time = System.currentTimeMillis();
                rnext_count = PCounter.rcounter;
                wnext_count = PCounter.wcounter;
                rcount = rnext_count - rprev_count;
                wcount = wnext_count - wprev_count;

                // THESE WILL NEED TO CHANGE TO GET ALL COUNTS
                mapCnt = Driver.usedDbMap.get(bigtName + "_" + type).getMapCnt();
                rowCnt = Driver.usedDbMap.get(bigtName + "_" + type).bigt.getRowCnt();
                columnCnt = Driver.usedDbMap.get(bigtName + "_" + type).bigt.getColumnCnt();
                countsUpToDate = true;
                System.out.println("Map Count : " + mapCnt);
                System.out.println("Row Count : " + rowCnt);
                System.out.println("Column Count : " + columnCnt);
                System.out.println("Write Count : " + wcount);
                System.out.println("Read Count : " + rcount);
                System.out.println("Time Taken : " + (next_time - prev_time));
            }
        }
    }

    public static void handleQuery(String[] tokens) throws Exception {
        String bigtName = tokens[1];
        String typeStr = tokens[2];
        String orderTypeStr = tokens[3];
        String rowFilter = tokens[4];
        String columnFilter = tokens[5];
        String valueFilter = tokens[6];
        String numbufStr = tokens[7];
        System.out.println(bigtName+"_"+Integer.parseInt(typeStr)+" "+ usedDbMap.size());
        BigDB bDB = usedDbMap.get(bigtName+"_"+typeStr);
        if(bDB == null){
            System.out.println("No BigTable found to query");
        }else
        //check that the integers are actually integers
        if(!isInteger(typeStr) || !isInteger(orderTypeStr) || !isInteger(numbufStr))
        {
            System.out.println("ERROR: TYPE ORDERTYPE and NUMBUF all must be valid integers.");
        }
        else
        {
            int type = Integer.parseInt(typeStr);
            int orderType = Integer.parseInt(orderTypeStr);
            int numbuf = Integer.parseInt(numbufStr);
            //check that typeStr is between 1-5
            if( type < 1 || type > 5 )
            {
                System.out.println("ERROR: The type must be an integer between 1 and 5");
            }
            //check that orderType is between 1 - 5
            else if( orderType < 1 || orderType > 5 )
            {
                System.out.println("ERROR: The order type must be an integer between 1 and 5");
            }
            //check that filters are valid
            else if( !isValidFilter(rowFilter) || !isValidFilter(columnFilter) || !isValidFilter(valueFilter))
            {
                System.out.println("ERROR: The filters must follow on of the 3 following patterns:");
                System.out.println("1) *\t\t2) <single value>\t\t3) [<from value>,<to value>]");
            }
            else
            {
                valueFilter = fixValFilterForInts(valueFilter);

                rprev_count = PCounter.rcounter;
                wprev_count = PCounter.wcounter;
                prev_time = System.currentTimeMillis();

                Iterator queryResults = bDB.bigt.openStream(orderType, rowFilter, columnFilter, valueFilter);

                Map m = queryResults.get_next();
                while(m != null)
                {
                    m.print();
                    m = queryResults.get_next();
                }
                queryResults.close();
                next_time = System.currentTimeMillis();
                rnext_count = PCounter.rcounter;
                wnext_count = PCounter.wcounter;
                rcount = rnext_count-rprev_count;
                wcount = wnext_count-wprev_count;
                System.out.println("Write Count : "+wcount);
                System.out.println("Read Count : "+rcount);
                System.out.println("Time Taken : "+(next_time-prev_time));
            }
        }
    }

    public static void handleGetCount(String[] tokens) {
        if (!isInteger(tokens[1])) {
            System.out.println("ERROR: The NUMBUF parameter must be an integer.");
        } else {
            int numbuf = Integer.parseInt(tokens[1]);

            if (!countsUpToDate) {
                // get counts
                mapCnt = 0;
                rowCnt = 0;
                columnCnt = 0;
                countsUpToDate = true;
            }
            System.out.println("Map Count : " + mapCnt);
            System.out.println("Row Count : " + rowCnt);
            System.out.println("Column Count : " + columnCnt);
        }
    }

    public static void handleMapInsert(String[] tokens) throws IOException {
        if (!isInteger(tokens[4]) || !isInteger(tokens[5]) || !isInteger(tokens[7])) {
            System.out.println("ERROR: The TS, TYPE, and NUMBUF parameter must be integers.");
        } else {
            String rowLabel = tokens[1];
            String columnLabel = tokens[2];
            String value = tokens[3];
            int timeStamp = Integer.parseInt(tokens[4]);
            int type = Integer.parseInt(tokens[5]);
            String bigtName = tokens[6];
            int numbuf = Integer.parseInt(tokens[7]);

            if (type < 1 || type > 5) {
                System.out.println("ERROR: The TYPE must be between 1 and 5.");
            } else {
                Map temp = new Map();
                temp.setRowLabel(rowLabel);
                temp.setColumnLabel(columnLabel);
                temp.setTimeStamp(timeStamp);
                temp.setValue(value);
                // insert into the bigtName table
                System.out.println("Eventually mapinsert will happen here");
            }

        }
    }

    public static void handleRowJoin(String[] tokens) {
        if (!isInteger(tokens[5])) {
            System.out.println("ERROR: The NUMBUF parameter must be an integer.");
        } else {
            String bigTName1 = tokens[1];
            String bigTName2 = tokens[2];
            String outBigT = tokens[3];
            String columnFilter = tokens[4];
            int numbuf = Integer.parseInt(tokens[5]);

            //do the RowJoin...
            System.out.println("Eventually rowJoin will happen here");
        }
    }

    public static void handleRowSort(String[] tokens) {
        if (!isInteger(tokens[4])) {
            System.out.println("ERROR: The NUMBUF parameter must be an integer.");
        } else {
            String inBigT = tokens[1];
            String outBigT = tokens[2];
            String columnName = tokens[3];
            int numbuf = Integer.parseInt(tokens[4]);

            //do rowsort
            System.out.println("Eventually rowSort will happen here");
        }
    }

    public static boolean isInteger(String s) 
    {
        if(s.isEmpty()) return false;
        for(int i = 0; i < s.length(); i++) 
        {
            if(!Character.isDigit(s.charAt(i))) 
            {
                return false;
            }
        }
        return true;
    }

    public static boolean isValidFilter(String s)
    {
        if(s.equals("*") || Pattern.matches("[^\\[\\],]+", s) || Pattern.matches("\\[[^\\[\\],]+,[^\\[\\],]+\\]", s))
        {
            return true;
        }
        else
        {
            return false;
        }
    }

    public static String fixValFilterForInts(String valFilter)
    {
        String pattern = "\\[(\\d+),(\\d+)\\]";
        Pattern r = Pattern.compile(pattern);
        Matcher m = r.matcher(valFilter);

        String patternEquality = "(\\d+)";
        Pattern rEquality = Pattern.compile(patternEquality);
        Matcher mEquality = rEquality.matcher(valFilter);
        if(m.find())
        {
            String fro = m.group(1);
            String to = m.group(2);

            String newFilter = "[" + padZeros(fro) + "," + padZeros(to) + "]";
            return newFilter;
        }
        else if(mEquality.find())
        {
            return padZeros(valFilter);
        }
        else
        {
            return valFilter;
        }
    }

    //pad with 0's
    public static String padZeros(String s)
    {
        int numZeros = GlobalConst.MAXVALUESIZE - s.length()-2;
        String zeros = "";
        for (int i = 0; i < numZeros; i++)
        {
            zeros += "0";
        }
        s = zeros + s;

        return s;
    }

    private static void printCommands()
    {
        System.out.println("batchinsert DATAFILENAME TYPE BIGTABLENAME NUMBUF");
        System.out.println("query BIGTABLENAME TYPE ORDERTYPE ROWFILTER COLUMNFILTER VALUEFILTER NUMBUF");
        System.out.println("getCounts NUMBUF");
        System.out.println("mapinsert RL CL VAL TS TYPE BIGTABLENAME NUMBUF");
        System.out.println("rowjoin BTNAME1 BTNAME2 OUTBTNAME COLUMNFILTER NUMBUF");
        System.out.println("rowsort INBTNAME OUTBTNAME COLUMNNAME NUMBUF");
        System.out.println("Type \"quit\" to quit\n");
    }
}