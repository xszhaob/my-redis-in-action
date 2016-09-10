import org.junit.Test;

import java.util.Random;

/**
 * author:xszhaobo
 * <p/>
 * date:2016/9/10
 * <p/>
 * package_name:PACKAGE_NAME
 * <p/>
 * project: my-redis-in-action
 */
public class RandomTest {

    @Test
    public void randomTest() {
        for (int i = 0; i < 1000; i++) {
            Random random = new Random();
            System.out.println(random.nextInt(10000));
        }
    }
}
