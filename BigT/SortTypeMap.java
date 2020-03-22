package BigT;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

public class SortTypeMap {
    public static List<Integer[]> val;

    public static void init(){
        val = new ArrayList<>();
        val.add(new Integer[]{0,1,2});
        val.add(new Integer[]{1,0,2});
        val.add(new Integer[]{0,2});
        val.add(new Integer[]{1,2});
        val.add(new Integer[]{2});
    }

    public static int[] returnSortOrderArray(int index){
        return Arrays.stream(val.get(index)).mapToInt(Integer::intValue).toArray();
    }
}
