package chapter05;

import org.junit.Test;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.Transaction;
import redis.clients.jedis.Tuple;
import redis.clients.jedis.ZParams;
import redis.clients.jedis.exceptions.InvalidURIException;

import java.math.BigDecimal;
import java.util.*;

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
        for (int i = 0; i < 100; i++) {
            updateStats(conn, "hit", "counter", i, 1000);
        }
    }

    @Test
    public void testGetStats() {
        Jedis conn = new Jedis("localhost");
        conn.select(15);
        Map<String, Double> stats = getStats(conn, "hit", "counter");
        System.out.println(stats);
    }

    /**
     * 存储统计数据的方法。
     * 统计数据使用的redis存储结构：有序集合
     * <p/>
     * 统计程序在写入数据之前会进行检查，
     * 确保被记录的是当前这一个小时的统计数据，
     * 并且将不属于当前这一小时的旧数据归档。
     * 此后，程序会构建两个临时有序集合，
     * 其中一个用于保存最小值，另一个则用于保存最大值。
     * 然后使用ZUNIONSTORE命令以及它的两个聚合函数
     * MIN和MAX，分别计算两个临时集合和当前统计数据集合
     * 之间的并集，最终获取当前的最大值和最小值。
     * 通过使用ZUNIONSTORE命令，程序可以快速地更新数据
     * 并且不需要使用WATCH监视可能会频繁更新的存储统计数据的键。
     * 在并集计算完成之后，删掉临时集合，并使用ZINCRBY对应统计数据中的
     * count、sum、sumsq三个成员进行更新。
     *
     * @param conn    redis连接
     * @param context 上下文
     * @param type    类型
     * @param value   统计数据值
     * @param timeOut 每次存储统计数据限定的超时时间（毫秒）
     * @return list或null
     * list.get(0) = 统计数据的总条数
     * list.get(1) = 统计数据值的总和
     * list.get(2) = 统计数据值的平方和
     */
    public List<Long> updateStats(Jedis conn, String context, String type, int value, long timeOut) {
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
            trans.zadd(tempKeyMin, value, "min");

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
            // 总数加1，并且把总数返回
            trans.zincrby(zKey, 1, "count");
            // 和加上本次值，并且把总和返回
            trans.zincrby(zKey, value, "sum");
            // 平方和加上本次值，返回平方和
            trans.zincrby(zKey, value * value, "sumsq");

            List<Object> exec = trans.exec();
            System.out.println(exec);
            if (exec == null) {
                continue;
            }
            List<Object> objects = exec.subList(exec.size() - 3, exec.size());
            List<Long> result = new ArrayList<Long>(objects.size());
            for (Object object : objects) {
                // 执行命令后返回的结果是double类型，转为int
                result.add(new BigDecimal(object.toString()).longValue());
            }
            return result;
        }
        return null;
    }

    /**
     * 获取给定上下文和类型的统计数据的信息。
     * 包括最大值、最小值、值的总和、数据总数量、平方和、平均数、标准差。
     *
     * @param conn    redis连接
     * @param context 上下文
     * @param type    统计数据类型
     * @return Map集合，包括最大值、最小值、值的总和、数据总数量、平方和、平均数、标准差
     */
    private Map<String, Double> getStats(Jedis conn, String context, String type) {
        String zKey = "stats:" + context + ":" + type;
        Set<Tuple> tupleSet = conn.zrangeWithScores(zKey, 0, -1);
        if (tupleSet == null || tupleSet.isEmpty()) {
            return null;
        }
        Map<String, Double> result = new HashMap<String, Double>(tupleSet.size());
        for (Tuple tuple : tupleSet) {
            result.put(tuple.getElement(), tuple.getScore());
        }
        result.put("average", result.get("sum") / result.get("count"));
        double numerator = result.get("sumsq") - Math.pow(result.get("sum"), 2) / result.get("count");
        double count = result.get("count");
        result.put("stddev", Math.pow(numerator / (count > 1 ? count - 1 : 1), .5));
        return result;
    }
}
