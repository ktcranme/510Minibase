package storage;

import BigT.*;

import java.io.IOException;
import java.util.List;

public class BigDBIterator extends Iterator {

    MultiTypeFileStream fs;
    int count_file;
    List<String> bigt;
    BigT tempBigt;

    public BigDBIterator(List<String> names) {
        try {
            bigt = names;
            count_file = 0;
            switch_file();
        } catch (Exception e){
            System.out.println("Error while initializing the BigDBIterator");
            e.printStackTrace();
        }
    }

    public Map get_next() throws Exception {
        Map tm = null;
        if(count_file>bigt.size()) {
            return null;
        }
        while((tm = fs.get_next())!=null){
            return tm;
        }
        switch_file();
        while(count_file<=bigt.size()){
            if((tm = fs.get_next())!=null) {
                return tm;
            } else {
                switch_file();
            }
        }
        return null;
    }

    private void switch_file() throws Exception {
        if(count_file!=0){
            fs.close();
            tempBigt.close();
        }

        if(count_file<bigt.size()) {
            tempBigt = new BigT(bigt.get(count_file));
            fs = new MultiTypeFileStream(tempBigt,null);
        }
        count_file++;
    }

    public void close() throws IOException {
        fs.close();
        tempBigt.close();
    }
}
