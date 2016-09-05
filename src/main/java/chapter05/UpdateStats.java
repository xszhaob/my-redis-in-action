package chapter05;

import redis.clients.jedis.Jedis;
import redis.clients.jedis.Pipeline;
import redis.clients.jedis.Transaction;
import redis.clients.jedis.ZParams;

import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.List;
import java.util.TimeZone;
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


    private List<Object> updateStats(Jedis conn, String context, String type, int value, long timeOut) {
        String zKey = "stats:" + context + ":" + type;
        String startKey = zKey + ":start";
        String start = Chapter05.ISO_FORMAT.format(new Date());
        conn.watch(startKey);
        String existing = conn.get(startKey);
        long end = System.currentTimeMillis() + timeOut;

        while (System.currentTimeMillis() < end) {
            Transaction trans = conn.multi();
            if (existing != null && Chapter05.COLLATOR.compare(existing, start) < 0) {
                trans.rename(zKey,zKey + ":last");
                trans.rename(startKey,zKey + ":pstart");
            }
            trans.set(startKey,start);

            String tempKeyMax = UUID.randomUUID().toString();
            String tempKeyMin = UUID.randomUUID().toString();

            trans.zadd(tempKeyMax, value, "max");
            trans.zadd(tempKeyMax, value, "min");

            trans.zunionstore(zKey,
                    new ZParams().aggregate(ZParams.Aggregate.MAX),
                    zKey, tempKeyMax);
            trans.zunionstore(zKey, new ZParams().aggregate(ZParams.Aggregate.MIN),
                    zKey, tempKeyMin);
            trans.del(tempKeyMax,tempKeyMin);
            trans.zincrby(zKey,1,"count");
            trans.zincrby(zKey,value,"sum");
            trans.zincrby(zKey,value * value,"sumsq");

            List<Object> exec = trans.exec();
            if (exec == null) {
                continue;
            }
            return exec.subList(exec.size() - 3,exec.size());
        }
        return null;
    }
}
