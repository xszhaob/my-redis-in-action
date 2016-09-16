package chapter06.lock;

import org.junit.Test;
import redis.clients.jedis.Jedis;
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
 * date:2016/9/15
 * <p/>
 * package_name:chapter06.lock
 * <p/>
 * project: my-redis-in-action
 * 分布式锁有类似地“首先获取锁， 然后执行操作，最后释放锁”的动作，
 * 但是分布式锁不在同一个进程的多线程中使用，甚至不是在同一个机器
 * 的多进程中使用，而是在不同的机器上的不同服务中获取和释放。
 * 我们可以利用redis来实现分布式锁。
 * redis提供的SETNX天生具有基本的加锁功能，但是它的功能并不完整，也
 * 不具备分布式锁常见的一些高级特性，所以我们需要自己动手实现一个高级的
 * 分布式事务锁。
 */
public class BetterLock {
    @Test
    public void stats_lock() {
        test(5);
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
                    for (int i1 = 0; i1 < 5; i1++) {
                        long start = System.currentTimeMillis();
                        acquireCount.incrementAndGet();
                        String aLock = acquireLockWithTimeOut(conn, "aLock", 100, 1);
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
        try {
            endLatch.await();
        } catch (InterruptedException ignore) {
        }
        executorService.shutdown();
        long count = 0;
        for (Long aLong : countList) {
            count += aLong;
        }
        System.out.println("并发量：" + threads + "，尝试获取锁" + acquireCount + "次，其中成功" + (acquireCount.get() - acquireFailCount.get()) + "次，获取锁平均耗时" + (count / (double) countList.size()) + "毫秒。");
    }


    /**
     * 获取锁。
     * 该获取锁方法有如下特性：
     * 1.如果获取锁成功，会设置锁的生存时间；
     * 2.虽然大多数情况下redis的锁都有生存时间，
     * 但是为了防止在上锁后、设置锁的生存周期
     * 之前获取锁的方法出现了异常而终止。我们加入如下判断：
     * 如果获取锁失败，会检查已存在锁是否设置有生存时间，
     * 如果没有设置生存时间，那么会给锁设置生存时间。
     * 。
     *
     * @param conn        redis连接
     * @param lockName    锁名称
     * @param waitTimeOut 等待获取锁的超时时间（毫秒）
     * @param lockTimeOut 锁的生存时间（秒）
     * @return 如果获取锁成功则返回锁键对应值，否则返回null
     */
    private String acquireLockWithTimeOut(Jedis conn, String lockName, long waitTimeOut, int lockTimeOut) {
        String lockKey = "lock:" + lockName;
        String lockId = UUID.randomUUID().toString();
        long end = System.currentTimeMillis() + waitTimeOut;
        int i = 0;
        while (System.currentTimeMillis() < end) {
            if (conn.setnx(lockKey, lockId) == 1) {
                conn.expire(lockKey, lockTimeOut);
                System.out.println("acquire lock '" + lockName + "',lockId=" + lockId + ",retry " + i);
                return lockId;
            }
            if (conn.ttl(lockKey) < 0) {
                conn.expire(lockKey, lockTimeOut);
            }
            try {
                Thread.sleep(1);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
            i++;
        }
        return null;
    }

    /**
     * 解锁。
     * 解锁时将判断锁键对应值是否是给定的值，防止误解锁。
     *
     * @param conn         redis连接
     * @param lockName     锁名称
     * @param lockId       锁键对应值
     * @param waiteTimeOut 解锁动作的超时时间（毫秒）
     * @return true如果解锁成功，否则返回false
     */
    private boolean releaseLock(Jedis conn, String lockName, String lockId, long waiteTimeOut) {
        String lockKey = "lock:" + lockName;
        long end = System.currentTimeMillis() + waiteTimeOut;
        int i = 0;
        while (System.currentTimeMillis() < end) {
            conn.watch(lockKey);
            if (lockId.equals(conn.get(lockKey))) {
                Transaction trans = conn.multi();
                trans.del(lockKey);
                List<Object> exec = trans.exec();
                if (exec != null) {
                    System.out.println("release lock '" + lockName + "',lockId=" + lockId + ",retry " + i);
                    return true;
                }
                i++;
                continue;
            }
            conn.unwatch();
            break;
        }
        return false;
    }
}
