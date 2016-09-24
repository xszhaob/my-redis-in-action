import org.junit.Test;

import java.util.HashSet;
import java.util.Iterator;
import java.util.Set;

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
        Set<Integer> intList = new HashSet<Integer>();
        intList.add(1);
        intList.add(2);
        intList.add(3);
        intList.add(5);
        intList.add(6);
        intList.add(7);
        for (Integer integer : intList) {
            System.out.println(integer);
        }
        Iterator<Integer> iterator = intList.iterator();
        while (iterator.hasNext()) {
            System.out.println(iterator.next());
        }
    }
}
