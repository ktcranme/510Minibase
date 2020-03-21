package driver;

import BigT.Iterator;
import BigT.Map;
import BigT.bigT;
import global.GlobalConst;

import java.io.*;
import java.util.regex.*;
import java.net.URL;
import java.util.ArrayList;
import java.util.List;

public class Driver {
    public static void main(String [] args) throws Exception {
        BufferedReader br = new BufferedReader(new InputStreamReader(System.in));
        System.out.println("Welcome to the BigTable interface");
        System.out.println("You have two options: BatchInsert and Query. Their structures follow:");
        System.out.println("batchinsert DATAFILENAME TYPE BIGTABLENAME");
        System.out.println("query BIGTABLENAME TYPE ORDERTYPE ROWFILTER COLUMNFILTER VALUEFILTER NUMBUF");
        System.out.println("\nType \"quit\" to quit\n");

        //begin reading in all the commands
        String command = br.readLine();
        while (!command.toLowerCase().equals("quit") && !command.toLowerCase().equals("q"))
        {
            String[] tokens = command.trim().split("\\s++");
            
            //batchinsert
            if(tokens[0].toLowerCase().equals("batchinsert") && tokens.length == 4)
            {
                String fileName = tokens[1];
                String typeStr = tokens[2];
                String bigtName = tokens[3];

                //check that type is a single character and digit
                if( !isInteger(typeStr) || typeStr.length() != 1)
                {
                    System.out.println("ERROR: TYPE must be a single digit 1 through 5");
                }
                else
                {
                    int type = Integer.parseInt(typeStr);

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
                        BatchInsert.batchinsert(fileName, type, bigtName);
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

                        //QUERY EXECUTED HERE
                        bigT queryBigT = new bigT(bigtName, type);
                        Iterator queryResults = queryBigT.openStream(orderType, rowFilter, columnFilter, valueFilter);

                        Map m = queryResults.get_next();
                        while(m != null)
                        {
                            m.print();
                            m = queryResults.get_next();
                        }
                    }
                }
            }
            //invalid command
            else
            {
                System.out.println("ERROR: The command you have entered does not match the corresponding number of parameters required.");
                System.out.println("The required structures are as follows:");
                System.out.println("batchinsert DATAFILENAME TYPE BIGTABLENAME");
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
}