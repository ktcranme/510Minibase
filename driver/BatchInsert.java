package driver;

import BigT.Map;
import BigT.bigT;
import btree.AddFileEntryException;
import btree.BTreeFile;
import btree.ConstructPageException;
import btree.GetFileEntryException;
import global.AttrType;
import global.GlobalConst;
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
            b1.insertMap(temp.getMapByteArray());
            //mapLs.add(temp);
            c++;
            System.out.println(c);
        }
        System.out.println(b1.getMapCnt());
        System.out.println(b1.getRowCnt());
        System.out.println(b1.getColumnCnt());
        System.out.println("Time taken"+(System.currentTimeMillis()-timeStart));
        //System.out.println(mapLs.size());
        //Map mapArr[] = mapLs.toArray(new Map[mapLs.size()]);
        //System.out.println(mapArr[0].getRowLabel());
        //insert code for batch insert
        //insert code for setting up bigt
    }

    //perform the batchinsertion but not in a main method
    public static void batchinsert(String fileName, int type, String bigtName, int numbuf) throws Exception
    {
        URL url = BatchInsert.class.getResource("/data/".concat(fileName));

        File csvFile = new File(url.getPath());
        BufferedReader read = new BufferedReader(new FileReader(csvFile));
        String line,rec[];
        Map temp;

        int c=0;
        BTreeFile tempbtf = null;
        if(type!=4){
            tempbtf = new BTreeFile("batch_insert_ind", AttrType.attrString, GlobalConst.MAXCOLUMNLABELSIZE + GlobalConst.MAXROWLABELSIZE + 1, 1);
        }

        bigT b1 = new bigT(bigtName,type);
        while ((line = read.readLine()) != null) {
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
            b1.insertMapBulk(temp.getMapByteArray(),tempbtf);
            
            c++;
            System.out.println(c);
        }
        tempbtf.destroyFile();
        System.out.println("Map Count : "+b1.getMapCnt());
        System.out.println("Column Count : "+b1.getColumnCnt());
        System.out.println("Row Count : "+b1.getRowCnt());
    }
}
