import org.junit.Test;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

/**
 * author:xszhaobo
 * <p/>
 * date:2016/9/5
 * <p/>
 * package_name:PACKAGE_NAME
 * <p/>
 * project: my-redis-in-action
 */
public class CollectionsTest {
    @Test
    public void binarySearch() {
        List<Integer> intList = new ArrayList<Integer>();
        intList.add(1);
        intList.add(2);
        intList.add(3);
        intList.add(5);
        intList.add(6);
        intList.add(7);
        Collections.sort(intList);
        System.out.println(intList.subList(0,0));

        int pre = 5;
        int bPre =  (int) Math.floor(pre / 60);
        System.out.println(bPre);
    }
}
