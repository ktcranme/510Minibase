package driver;

import BigT.Map;
import BigT.bigT;
import btree.AddFileEntryException;
import btree.ConstructPageException;
import btree.GetFileEntryException;
import global.SystemDefs;
import heap.HFBufMgrException;
import heap.HFDiskMgrException;
import heap.HFException;

import java.io.*;
import java.net.URL;
import java.util.ArrayList;
import java.util.List;

public class BatchInsert {
    //insert your data file in /data folder before running the make command, to include the data file in classpath.
    public static void main(String[] args) throws Exception {
        BufferedReader br = new BufferedReader(new InputStreamReader(System.in));
        System.out.print("DATAFILENAME(ensure that it is placed in /driver/data/):");
        String fileName = br.readLine();
        System.out.print("TYPE(1,2,3,4,5):");
        int type = Integer.parseInt(br.readLine());
        System.out.print("BIGTABLENAME:");
        String bigtName = br.readLine();
        URL url = BatchInsert.class.getResource("/data/".concat(fileName));
        File csvFile = new File(url.getPath());
        BufferedReader read = new BufferedReader(new FileReader(csvFile));
        String line,rec[];
        Map temp;
        List<Map> mapLs = new ArrayList<>();
        int c=0;
        String dbpath = "D:\\minibase_db\\"+"hf"+System.getProperty("user.name")+".minibase-db";
        SystemDefs sysdef = new SystemDefs(dbpath,100000,100,"Clock");
        bigT b1 = new bigT(bigtName,type);
        long timeStart = System.currentTimeMillis();
        while ((line = read.readLine()) != null) {
            long x = System.currentTimeMillis();
            rec = line.split(",");
            temp = new Map();
            if(c==0) {
                temp.setRowLabel(rec[0].substring(1));
            }
            else {
                temp.setRowLabel(rec[0]);
            }
            temp.setColumnLabel(rec[1]);
            temp.setTimeStamp(Integer.parseInt(rec[3]));
            temp.setValue(rec[2]);
            // System.out.println("Time taken to create map: "+(System.currentTimeMillis()-x));
            // temp.print();
            // if ( SystemDefs.JavabaseBM.getNumUnpinnedBuffers() != SystemDefs.JavabaseBM.getNumBuffers() ) {
            //     System.err.println ("*** The heap-file scan has not unpinned " + "its page after finishing\n");
            //     System.err.println("Unpinned: " + SystemDefs.JavabaseBM.getNumUnpinnedBuffers() + ", Total: " + SystemDefs.JavabaseBM.getNumBuffers());
            //     throw new Exception();
            // }
            // System.out.println("");
            b1.insertMap(temp.getMapByteArray());
            //mapLs.add(temp);
            c++;
            // System.out.println("");
            // if (c % 100 == 0)
            System.out.println(c);
        }
        System.out.println(b1.getMapCnt());
        System.out.println("Total Time taken: "+(System.currentTimeMillis()-timeStart));
        //System.out.println(mapLs.size());
        //Map mapArr[] = mapLs.toArray(new Map[mapLs.size()]);
        //System.out.println(mapArr[0].getRowLabel());
        //insert code for batch insert
        //insert code for setting up bigt
    }
}
