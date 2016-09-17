package chapter06.semaphore;

import org.junit.Test;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.Transaction;

import java.util.List;
import java.util.UUID;
import java.util.Vector;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * author:xszhaobo
 * <p/>
 * date:2016/9/16
 * <p/>
 * package_name:chapter06.semaphore
 * <p/>
 * project: my-redis-in-action
 * 利用redis实现一个简单的信号量，
 * 这个基本的信号量可以两个良好地工作。
 * 但是存在一个问题：它在获取信号量的时候，
 * 会假设每个服务访问到的系统时间都是相同的，
 * 而在不同的主机中可能并非如此。
 * 举个例子，对于系统A和B来说，假如A的系统时间比B的系统时间快10毫秒，
 * 那么当A获取了最后一个信号量时，B只需要在10毫秒内获取信号量，
 * 就可以在A不知情的情况下“偷走”A已经获取的信号量。
 */
public class BaseSemaphore {

    @Test
    public void stats_lock() {
        test(50);
    }

    /**
     * 分布式锁的测试方法
     *
     * @param threads 模拟获取锁的请求线程数
     */
    public void test(int threads) {
        final AtomicInteger acquireFailCount = new AtomicInteger();
        final AtomicInteger acquireCount = new AtomicInteger();

        final CountDownLatch latch = new CountDownLatch(0);
        final CountDownLatch endLatch = new CountDownLatch(threads);
        final List<Long> countList = new Vector<Long>();
        ExecutorService executorService = Executors.newFixedThreadPool(threads);
        for (int i = 0; i < threads; i++) {
            executorService.execute(new Runnable() {
                public void run() {
                    final Jedis conn = new Jedis("localhost");
                    conn.select(0);
                    try {
                        latch.await();
                    } catch (InterruptedException e) {
                        e.printStackTrace();
                    }
                    for (int i1 = 0; i1 < 50; i1++) {
                        long start = System.currentTimeMillis();
                        acquireCount.incrementAndGet();
                        String aLock = acquireSemaphore(conn, "aSem", 5, 1);
                        if (aLock != null) {
                            countList.add(System.currentTimeMillis() - start);
                            releaseSemaphore(conn, "aSem", aLock);
                        } else {
                            acquireFailCount.incrementAndGet();
                        }
                    }
                    endLatch.countDown();
                }
            });
        }
        latch.countDown();
        try {
            endLatch.await();
        } catch (InterruptedException ignore) {
        }
        executorService.shutdown();
        long count = 0;
        for (Long aLong : countList) {
                count += aLong;
        }
        System.out.println("并发量：" + threads + "，尝试获取信号量" + acquireCount + "次，其中成功" + (acquireCount.get() - acquireFailCount.get()) + "次，获取锁平均耗时" + (count / (double) countList.size()) + "毫秒。");
    }

    /**
     * 获取一个信号量
     *
     * @param conn    redis连接
     * @param semName 信号量名称
     * @param limit   信号量限制数
     * @param timeOut 获取的信号量过期时间
     * @return 信号量标识
     */
    private String acquireSemaphore(Jedis conn, String semName, int limit, long timeOut) {
        String member = UUID.randomUUID().toString();
        long now = System.currentTimeMillis();

        Transaction trans = conn.multi();
        // 先删除过期的信号量
        trans.zremrangeByScore(semName, 0, now - timeOut);
        trans.zadd(semName, now, member);
        trans.zrank(semName, member);
        List<Object> exec = trans.exec();
        if (exec != null && (Long) (exec.get(exec.size() - 1)) < limit) {
            return member;
        }
        return null;
    }

    /**
     * 删除信号量
     *
     * @param conn    redis连接
     * @param semName 信号量名称
     * @param id      信号量标识
     */
    private void releaseSemaphore(Jedis conn, String semName, String id) {
        conn.zrem(semName, id);
    }
}
