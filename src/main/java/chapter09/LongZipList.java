package chapter09;

import org.junit.Test;
import redis.clients.jedis.DebugParams;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.Pipeline;

import javax.sound.midi.Soundbank;

/**
 * author:xszhaobo
 * <p/>
 * date:2016/11/3
 * <p/>
 * package_name:chapter09
 * <p/>
 * project: my-redis-in-action
 */
public class LongZipList {
//    public static void main(String[] args) {
//        Jedis conn = new Jedis();
//        double costTime = longZipListPerformance(conn, "game", 5, 10, 10);
//        System.out.println("costTime:" + costTime + "ms");
//        System.out.println(conn.llen("game"));
//    }


    public static double longZipListPerformance(Jedis conn, String key, int length, int passes, int psize) {
        conn.del(key);
        for (int i = 0; i < length; i++) {
            conn.rpush(key,String.valueOf(i));
        }

        Pipeline pipelined = conn.pipelined();
        long start = System.currentTimeMillis();
        for (int i = 0; i < passes; i++) {
            for (int i1 = 0; i1 < psize; i1++) {
                pipelined.rpoplpush(key,key);
            }
            pipelined.sync();
        }
        return (passes * psize) / (System.currentTimeMillis() -start);
    }

    /**
     * 由测试可以看出，在日常应用中压缩链表用处应该并不广泛。
     * 首先内存容量已经不是大问题；
     * 其次没有多少程序员会在乎这个，因为很多都不知道这种原理。
     */
    @Test
    public void zipListTest() {
        Jedis conn = new Jedis();
        conn.del("game");
        conn.rpush("game","a","b","c","d");
        // conn.debug() 查看特定对应的相关信息
        DebugParams params = DebugParams.OBJECT("game");
        String debug = conn.debug(params);
        // Value at:00007FD851C6E290 refcount:1 encoding:ziplist
        // serializedlength:24 lru:1791026 lru_seconds_idle:0
        /*
        encoding代表这个对象的编码为压缩列表，这个压缩列表占用24个字节
         */
        System.out.println(debug);

        // Value at:00007FD851C6E290 refcount:1 encoding:ziplist
        // serializedlength:36 lru:1791026 lru_seconds_idle:0
        /*
        向列表中推入4个元素之后，对应的编码依旧是压缩列表，
        只是体积增长到了36个字节（推入的4个元素，每个元素都需要花费
        一个字节存储，并带来两个字节的额外开销）
         */
        conn.rpush("game","e","f","g","h");
        String debug1 = conn.debug(params);
        System.out.println(debug1);

        // Value at:00007FD851C6E290 refcount:1 encoding:linkedlist
        // serializedlength:30 lru:1791026 lru_seconds_idle:0
        /*
        当一个超过编码允许大小的元素被推入列表里面的时候，
        列表将从压缩列表编码转化标准的链表
         */
        StringBuilder val65 = new StringBuilder();
        for (int i = 0; i < 65; i++) {
            val65.append("i");
        }
        conn.rpush("game",val65.toString());
        System.out.println(conn.debug(params));

        // Value at:00007FD851C6E290 refcount:1 encoding:linkedlist
        // serializedlength:17 lru:1791026 lru_seconds_idle:0
        /*
        当压缩列表被转换为普通的结构之后，即使结构将来重新满足配置
        选项设置的限制条件，结构不也会重新转换为压缩列表。
         */
        String game = conn.rpop("game");
        System.out.println(game);
        System.out.println(conn.debug(params));
    }
}
