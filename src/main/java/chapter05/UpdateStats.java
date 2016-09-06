package chapter05;

import org.junit.Test;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.Transaction;
import redis.clients.jedis.ZParams;

import java.util.Date;
import java.util.List;
import java.util.UUID;

/**
 * author:xszhaobo
 * <p/>
 * date:2016/9/5
 * <p/>
 * package_name:chapter05
 * <p/>
 * project: my-redis-in-action
 */
public class UpdateStats {

    @Test
    public void testUpdateStats() {
        Jedis conn = new Jedis("localhost");
        conn.select(15);
        UpdateStats updateStats = new UpdateStats();
        for (int i = 0; i < 100; i++) {
            updateStats.updateStats(conn, "hit", "counter", i, 1000);
        }
    }

    private List<Object> updateStats(Jedis conn, String context, String type, int value, long timeOut) {
        String zKey = "stats:" + context + ":" + type;
        String startKey = zKey + ":start";
        String start = Chapter05.ISO_FORMAT.format(new Date());
        conn.watch(startKey);
        String existing = conn.get(startKey);
        long end = System.currentTimeMillis() + timeOut;

        while (System.currentTimeMillis() < end) {
            Transaction trans = conn.multi();
            // 如果key时上个小时的，那么先归档，然后重新设置当前小时的key
            if (existing != null && Chapter05.COLLATOR.compare(existing, start) < 0) {
                trans.rename(zKey, zKey + ":last");
                trans.rename(startKey, zKey + ":pstart");
            }
            trans.set(startKey, start);

            // 设置临时值
            String tempKeyMax = UUID.randomUUID().toString();
            String tempKeyMin = UUID.randomUUID().toString();

            trans.zadd(tempKeyMax, value, "max");
            trans.zadd(tempKeyMax, value, "min");

            // 获取并集，并且分值以最大、最小的为准
            /*
            使用聚合函数MIN和MAX，
            对存储统计数据的键以及两个临时键进行并集计算
             */
            trans.zunionstore(zKey,
                    new ZParams().aggregate(ZParams.Aggregate.MAX),
                    zKey, tempKeyMax);
            trans.zunionstore(zKey, new ZParams().aggregate(ZParams.Aggregate.MIN),
                    zKey, tempKeyMin);
            // 删除两个临时key
            trans.del(tempKeyMax, tempKeyMin);
            // 总数加1
            trans.zincrby(zKey, 1, "count");
            // 和加上本次值
            trans.zincrby(zKey, value, "sum");
            // 平方和加上本次值
            trans.zincrby(zKey, value * value, "sumsq");

            List<Object> exec = trans.exec();
            System.out.println(exec);
            if (exec == null) {
                continue;
            }
            System.out.println(conn.get(startKey));
            return exec.subList(exec.size() - 3, exec.size());
        }
        return null;
    }
}
