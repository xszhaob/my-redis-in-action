import redis.clients.jedis.Jedis;
import redis.clients.jedis.Pipeline;

import java.util.Set;

/**
 * author:xszhaobo
 * <p/>
 * date:2016/10/16
 * <p/>
 * package_name:PACKAGE_NAME
 * <p/>
 * project: my-redis-in-action
 */
public class CleanKeys {

    public static void main(String[] args) {
        long start = System.currentTimeMillis();
        Jedis conn = new Jedis();
        Set<String> keys = conn.keys("*");
        Pipeline pip = conn.pipelined();
        int count = 0;
        for (String key : keys) {
            pip.del(key);
            if (++count%10 == 0) {
                pip.sync();
            }
        }
        pip.sync();
        System.out.println(System.currentTimeMillis() - start);
    }
}
