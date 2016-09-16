package chapter06.lock;

import org.junit.Test;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.Response;
import redis.clients.jedis.Transaction;

import java.util.ArrayList;
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
        final List<Long> countList = new Vector<Long>();
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
                        String aLock = acquireLock(conn, "aLock", 100);
                        if (aLock != null) {
                            countList.add(System.currentTimeMillis() - start);
                            releaseLock(conn, "aLock", aLock, 100);
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
     * @return 如果获取锁成功则返回锁键对应的值，否则返回null
     */
    private String acquireLock(Jedis conn, String lockName, long timeOut) {
        String lockKey = "lock:" + lockName;
        String uuid = UUID.randomUUID().toString();
        long end = System.currentTimeMillis() + timeOut;
        while (System.currentTimeMillis() < end) {
            if (conn.setnx(lockKey, uuid) == 1) {
                return uuid;
            }
            try {
                Thread.sleep(1);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
        }
        return null;
    }

    /**
     * 释放锁
     *
     * @param conn     redis连接
     * @param lockName 锁名称
     * @param timeOut  超时时间
     * @return true如果释放锁，否则返回false
     */
    private boolean releaseLock(Jedis conn, String lockName,String lockId, long timeOut) {
        String lockKey = "lock:" + lockName;
        long end = System.currentTimeMillis() + timeOut;
        while (System.currentTimeMillis() < end) {
            conn.watch(lockKey);
            if (lockId.equals(conn.get(lockKey))) {
                Transaction trans = conn.multi();
                Response<Long> del = trans.del(lockKey);
                if (del == null) {
                    continue;
                }
                return true;
            }
            conn.unwatch();
            break;
        }
        return false;
    }
}
