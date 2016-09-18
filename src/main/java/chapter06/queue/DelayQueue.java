package chapter06.queue;

import redis.clients.jedis.Jedis;
import redis.clients.jedis.Tuple;

import java.util.Iterator;
import java.util.Set;
import java.util.UUID;

/**
 * author:xszhaobo
 * <p/>
 * date:2016/9/18
 * <p/>
 * package_name:chapter06.queue
 * <p/>
 * project: my-redis-in-action
 * 具有延迟特性的队列
 */
public class DelayQueue {


    /**
     * 把需要在未来某个时间执行的任务放入延迟任务队列
     *
     * @param conn  redis连接
     * @param queue 执行任务队列
     * @param name  回调函数名称
     * @param args  回调函数参数
     * @param delay 延迟时间
     * @return 返回放入延迟队列的id，否则为null
     */
    private String executeLater(Jedis conn, String queue, String name, String args, long delay) {
        String id = UUID.randomUUID().toString();
        String member = id + "&&" + queue + "&&" + name + "&&" + args;
        if (delay > 0) {
            conn.zadd("delayed:", System.currentTimeMillis() + delay, member);
        } else {
            conn.rpush("queue:" + queue, member);
        }
        return id;
    }

    /**
     * 获取要执行的任务队列
     *
     * @param conn jedis连接
     */
    private void pollQueue(Jedis conn) {
        while (true) {
            Set<Tuple> tuples = conn.zrangeWithScores("delayed:", 0, 0);
            if (tuples != null && tuples.size() > 0) {
                Tuple next = tuples.iterator().next();
                if (next.getScore() > System.currentTimeMillis()) {
                    continue;
                }
                String member = next.getElement();
                int idIndex = member.indexOf("&&");
                String id = member.substring(0, idIndex);
                String queue = member.substring(idIndex + 2, member.indexOf("&&", idIndex + 2));
                conn.rpush("queue:" + queue, id);
            }
        }
    }
}
