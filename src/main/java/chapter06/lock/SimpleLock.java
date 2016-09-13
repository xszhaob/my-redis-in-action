package chapter06.lock;

import org.junit.Test;
import redis.clients.jedis.Jedis;

import java.util.ArrayList;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * author:xszhaobo
 * <p/>
 * date:2016/9/13
 * <p/>
 * package_name:chapter06.lock
 * <p/>
 * project: my-redis-in-action
 * 使用redis构建一个简单的锁
 */
public class SimpleLock {

    @Test
    public void test() throws InterruptedException {
        final AtomicInteger acquireFailCount = new AtomicInteger();

        final CountDownLatch latch = new CountDownLatch(0);
        final CountDownLatch endLatch = new CountDownLatch(100);
        final List<Long> countList = new ArrayList<Long>();
        ExecutorService executorService = Executors.newFixedThreadPool(100);
        for (int i = 0; i < 100; i++) {
            executorService.execute(new Runnable() {
                public void run() {
                    final Jedis conn = new Jedis("localhost");
                    conn.select(0);
                    try {
                        latch.await();
                    } catch (InterruptedException e) {
                        e.printStackTrace();
                    }
                    for (int i1 = 0; i1 < 10; i1++) {
                        long start = System.currentTimeMillis();
                        if (acquireLock(conn,"aLock",100)) {
                            countList.add(System.currentTimeMillis() - start);
                            releaseLock(conn,"aLock",100);
                        } else {
                            acquireFailCount.incrementAndGet();
                        }
                    }
                    endLatch.countDown();
                }
            });
        }
        latch.countDown();
        endLatch.await();
        executorService.shutdown();
        long count = 0;
        for (Long aLong : countList) {
            count += aLong;
        }
        System.out.println("获取锁平均耗时" + (count/(double)countList.size()) + "毫秒，失败" + acquireFailCount + "次");
    }


    /**
     * 获取锁
     *
     * @param conn     redis连接
     * @param lockName 锁名称
     * @param timeOut  超时时间
     * @return true如果获取锁，否则返回false
     */
    private boolean acquireLock(Jedis conn, String lockName, long timeOut) {
        String lockKey = "lock:" + lockName;
        String uuid = UUID.randomUUID().toString();
        long end = System.currentTimeMillis() + timeOut;
        while (System.currentTimeMillis() < end) {
            Long result = conn.setnx(lockKey, uuid);
            if (result == 1) {
                conn.expire(lockKey, 10);
                return true;
            }
        }
        return false;
    }

    /**
     * 释放锁
     *
     * @param conn     redis连接
     * @param lockName 锁名称
     * @param timeOut  超时时间
     * @return true如果释放锁，否则返回false
     */
    private boolean releaseLock(Jedis conn, String lockName, long timeOut) {
        String lockKey = "lock:" + lockName;
        long end = System.currentTimeMillis() + timeOut;
        while (System.currentTimeMillis() < end) {
            Long result = conn.del(lockKey);
            if (result == 1) {
                return true;
            }
        }
        conn.expire(lockKey,1);
        return false;
    }
}
