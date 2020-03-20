package driver;

import BigT.Map;

import java.io.*;
import java.net.URL;
import java.util.ArrayList;
import java.util.List;

public class BatchInsert {
    //insert your data file in /data folder before running the make command, to include the data file in classpath.
    public static void main(String[] args) throws IOException {
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
        while ((line = read.readLine()) != null) {
            rec = line.split(",");
            if(rec[0].length()>16 && rec[1].length()>16 && rec[3].length()>16)
                continue;
            temp = new Map();
            if(c==0) {
                c=1;
                temp.setRowLabel(rec[0].substring(1));
            }
            else {
                temp.setRowLabel(rec[0]);
            }
            temp.setColumnLabel(rec[1]);
            temp.setTimeStamp(Integer.parseInt(rec[2]));
            temp.setValue(rec[3]);
            mapLs.add(temp);
        }
        System.out.println(mapLs.size());
        Map mapArr[] = mapLs.toArray(new Map[mapLs.size()]);
        //System.out.println(mapArr[0].getRowLabel());
        //insert code for batch insert
        //insert code for setting up bigt
    }
}
