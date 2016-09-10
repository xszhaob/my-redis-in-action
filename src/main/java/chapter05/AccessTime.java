package chapter05;

import org.junit.Test;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.Transaction;

import java.util.List;
import java.util.Random;
import java.util.UUID;

/**
 * author:xszhaobo
 * <p/>
 * date:2016/9/8
 * <p/>
 * package_name:chapter05
 * <p/>
 * project: my-redis-in-action
 * 记录和计算访问时长
 */
public class AccessTime {

    @Test
    public void accessTimeTest() {
        Jedis conn = new Jedis("localhost");
        conn.select(15);
        for (int i = 0; i < 1000; i++) {
            accessTime(conn, UUID.randomUUID().toString());
        }
    }

    /**
     * 统计访问时间的方法
     *
     * @param conn jedis连接
     * @param context 上下文
     */
    private void accessTime(Jedis conn, String context) {
        long start = System.currentTimeMillis();
        processSomething();
        long timeConsuming = System.currentTimeMillis() - start;

        List<Long> stats = new UpdateStats().updateStats(conn, context, "accessTime", (int) timeConsuming, 1000);
        long average = stats.get(1) / stats.get(0);
        Transaction trans = conn.multi();
        trans.zadd("slowest:accessTime", average, context);
        trans.zremrangeByRank("slowest:accessTime", 0, -101);
        // 执行事务模块中的命令
        trans.exec();
    }

    // 模拟处理请求的方法
    private void processSomething() {
        System.out.println("process something.");
        try {
            Random random = new Random();
            Thread.sleep(random.nextInt(1000));
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
    }
}
