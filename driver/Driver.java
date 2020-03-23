package driver;

import BigT.Iterator;
import BigT.Map;
import BigT.bigT;
import diskmgr.BigDB;
import diskmgr.PCounter;
import global.GlobalConst;
import global.SystemDefs;

import java.io.*;
import java.util.regex.*;
import java.net.URL;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;

public class Driver {
    public static java.util.Map<String, BigDB> usedDbMap;
    public static void main(String [] args) throws Exception {
        usedDbMap = new HashMap<>();

        String dbpath = "D:\\minibase_db\\"+"hf"+System.getProperty("user.name")+".minibase-db";
        if (args.length > 0 && args[0].equals("R"))
            SystemDefs.MINIBASE_RESTART_FLAG = true;

        SystemDefs sysdef = new SystemDefs(dbpath,100000,500,"Clock");;
        BufferedReader br = new BufferedReader(new InputStreamReader(System.in));
        System.out.println("Welcome to the BigTable interface");
        System.out.println("You have two options: BatchInsert and Query. Their structures follow:");
        System.out.println("batchinsert DATAFILENAME TYPE BIGTABLENAME NUMBUF");
        System.out.println("query BIGTABLENAME TYPE ORDERTYPE ROWFILTER COLUMNFILTER VALUEFILTER NUMBUF");
        System.out.println("\nType \"quit\" to quit\n");
        int rprev_count, wprev_count, rnext_count, wnext_count, wcount, rcount;
        long prev_time,next_time;
        PCounter.initialize();
        //begin reading in all the commands
        String command = br.readLine();
        while (!command.toLowerCase().equals("quit") && !command.toLowerCase().equals("q"))
        {
            String[] tokens = command.trim().split("\\s++");

            //batchinsert
            if(tokens[0].toLowerCase().equals("batchinsert") && tokens.length == 5)
            {
                String fileName = tokens[1];
                String typeStr = tokens[2];
                String bigtName = tokens[3];
                String numbufStr = tokens[4];

                //check that type is a single character and digit
                if( !isInteger(typeStr) || typeStr.length() != 1)
                {
                    System.out.println("ERROR: TYPE must be a single digit 1 through 5");
                }
                else if ( !isInteger(numbufStr) )
                {
                    System.out.println("ERROR: NUMBUF must be an integer");
                }
                else
                {
                    int type = Integer.parseInt(typeStr);
                    int numbuf = Integer.parseInt(numbufStr);

                    //check that data file exists
                    URL url = Driver.class.getResource("/data/".concat(fileName));
                    if( url == null )
                    {
                        System.out.println("ERROR: " + fileName + " does not exist in the /data/ folder.");
                    }
                    //check that the type is only values 1-5
                    else if( type < 1 || type > 5 )
                    {
                        System.out.println("ERROR: The type must be an integer between 1 and 5");
                    }
                    else
                    {
                        rprev_count = PCounter.rcounter;
                        wprev_count = PCounter.wcounter;
                        prev_time = System.currentTimeMillis();

                        sysdef = new SystemDefs(dbpath,100000,numbuf,"Clock");
                        BatchInsert.batchinsert(fileName, type, bigtName, numbuf);

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
            //query
            else if(tokens[0].toLowerCase().equals("query") && tokens.length == 8)
            {
                String bigtName = tokens[1];
                String typeStr = tokens[2];
                String orderTypeStr = tokens[3];
                String rowFilter = tokens[4];
                String columnFilter = tokens[5];
                String valueFilter = tokens[6];
                String numbufStr = tokens[7];

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

                        bigT queryBigT = null;
                        Iterator queryResults = null;
                        SystemDefs.JavabaseBM.flushAllPages();
                        try {
                            // System.out.println("Doin resize");
                            // // SystemDefs.JavabaseBM.resize(numbuf);
                            SystemDefs.MINIBASE_RESTART_FLAG = true;
                            sysdef = new SystemDefs(dbpath, 100000, numbuf, "Clock");
                            // System.out.println("Fin resize");

                            // QUERY EXECUTED HERE
                            // queryBigT = new bigT(bigtName, type);

                            System.out.println(bigtName+"_"+Integer.parseInt(typeStr)+" "+ usedDbMap.size());
                            BigDB bDB = usedDbMap.get(bigtName);
                            if(bDB == null){
                                System.out.println("No BigTable found to query. Checking persistent data.");
                                bDB = new BigDB(Integer.parseInt(typeStr));
                                bDB.initBigT(bigtName);
            
                                Driver.usedDbMap.put(bigtName, bDB);
                            }

                            System.out.println("Querying from " + bDB.name);

                            queryResults = bDB.bigt.openStream(orderType, rowFilter, columnFilter, valueFilter);

                            Map m = queryResults.get_next();
                            while (m != null) {
                                m.print();
                                m = queryResults.get_next();
                            }
                            next_time = System.currentTimeMillis();
                            rnext_count = PCounter.rcounter;
                            wnext_count = PCounter.wcounter;
                            rcount = rnext_count - rprev_count;
                            wcount = wnext_count - wprev_count;
                            System.out.println("Write Count : " + wcount);
                            System.out.println("Read Count : " + rcount);
                            System.out.println("Time Taken : " + (next_time - prev_time));
                            // queryResults.close();
                            if (bDB.bigt.btf != null)
                                bDB.bigt.btf.close();
                            if (bDB.bigt.btfTS != null)
                                bDB.bigt.btfTS.close();
                            bDB.bigt.tmp.close();
                            // SystemDefs.JavabaseBM.flushAll();
                            // SystemDefs.JavabaseBM.flushAllPages();

                            if (queryResults != null) {
                                queryResults.close();
                                // queryBigT.btf.close();
                                // queryBigT.btfTS.close();
                                // queryBigT.tmp.close();
                                SystemDefs.JavabaseBM.flushAllPages();
                            }
                        } catch (Exception e) {
                            e.printStackTrace();
                            SystemDefs.MINIBASE_RESTART_FLAG = true;
                            sysdef = new SystemDefs(dbpath, 100000, numbuf, "Clock");
                        } finally {
                            // if (queryResults != null) {
                            //     queryResults.close();
                            //     // queryBigT.btf.close();
                            //     // queryBigT.btfTS.close();
                            //     // queryBigT.tmp.close();
                            //     SystemDefs.JavabaseBM.flushAllPages();
                            // }
                        }
                    
                        System.out.println("Buffer State: " + SystemDefs.JavabaseBM.getNumUnpinnedBuffers() + "/" + SystemDefs.JavabaseBM.getNumBuffers());
                    
                    }
                }
            }
            //invalid command
            else
            {
                System.out.println("ERROR: The command you have entered does not match the corresponding number of parameters required.");
                System.out.println("The required structures are as follows:");
                System.out.println("batchinsert DATAFILENAME TYPE BIGTABLENAME NUMBUF");
                System.out.println("query BIGTABLENAME TYPE ORDERTYPE ROWFILTER COLUMNFILTER VALUEFILTER NUMBUF");
                System.out.println("\n");
            }

            System.out.println("Next command:\n");
            command = br.readLine();
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
}