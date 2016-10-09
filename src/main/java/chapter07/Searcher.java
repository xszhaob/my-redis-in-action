package chapter07;

import redis.clients.jedis.Jedis;
import redis.clients.jedis.Transaction;

import java.lang.reflect.Array;
import java.lang.reflect.InvocationTargetException;
import java.util.*;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Created by Bo.Zhao on 2016/10/9.
 */
public class Searcher {
    public static final Pattern WORD_RE = Pattern.compile("[a-z']{2,}");
    public static final Set<String> STOP_WORDS = new HashSet<String>();

    static {
        String[] split = ("able about across after all almost also am among " +
                "an and any are as at be because been but by can " +
                "cannot could dear did do does either else ever " +
                "every for from get got had has have he her hers " +
                "him his how however if in into is it its just " +
                "least let like likely may me might most must my " +
                "neither no nor not of off often on only or other " +
                "our own rather said say says she should since so " +
                "some than that the their them then there these " +
                "they this tis to too twas us wants was we were " +
                "what when where which while who whom why will " +
                "with would yet you your").split(" ");
        Collections.addAll(STOP_WORDS, split);
    }


    /**
     * 标记文档
     *
     * @param content 文档内容
     * @return 文档中除去1个字符和常用单子之外的单词组成的集合
     */
    public Set<String> tokenize(String content) {
        Set<String> result = new HashSet<String>();
        Matcher matcher = WORD_RE.matcher(content);
        while (matcher.find()) {
            String word = matcher.group().trim();
            if (word.length() > 2 && !STOP_WORDS.contains(word)) {
                result.add(word);
            }
        }
        return result;
    }


    public int indexDocument(Jedis conn, String docId, String content) {
        Set<String> tokenize = tokenize(content);
        Transaction trans = conn.multi();
        for (String s : tokenize) {
            trans.sadd("idx:" + s, docId);
        }
        return trans.exec().size();
    }

    /**
     * 对集合进行交集、并集、差集计算的辅助函数
     *
     * @param trans  传入的事务流水线
     * @param method 需要执行的集合操作
     * @param ttl    设置计算结果缓存的过期时间
     * @param items  操作的集合
     * @return 缓存的key
     * @throws NoSuchMethodException
     * @throws InvocationTargetException
     * @throws IllegalAccessException
     */
    public String setCommon(Transaction trans, String method, int ttl, String... items) {
        String[] keys = new String[items.length];
        System.arraycopy(items, 0, keys, 0, items.length);
        String id = UUID.randomUUID().toString();
        try {
            trans.getClass().getDeclaredMethod(method, String.class, String[].class).invoke(trans, "idx:" + id, keys);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
        trans.expire("idx:" + id, ttl);
        trans.exec();
        return id;
    }


    public String intersect(Transaction trans, int ttl, String... items) {
        return setCommon(trans, "sinterstore", ttl, items);
    }

    public String union(Transaction trans, int ttl, String... items) {
        return setCommon(trans, "sunionstore", ttl, items);
    }

    public String difference(Transaction trans, int ttl, String... items) {
        return setCommon(trans, "sdiffstore", ttl, items);
    }


}
