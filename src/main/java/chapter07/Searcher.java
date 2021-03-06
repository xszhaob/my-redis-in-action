package chapter07;

import redis.clients.jedis.Jedis;
import redis.clients.jedis.Transaction;
import redis.clients.jedis.ZParams;

import java.lang.reflect.InvocationTargetException;
import java.util.*;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Created by Bo.Zhao on 2016/10/9.
 */
public class Searcher {
    public static final Pattern QUERY_RE = Pattern.compile("[+-]?[a-z']{2,}]");
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


    public Query parse(String queryString) {
        Query query = new Query();
        Matcher matcher = QUERY_RE.matcher(queryString);
        Set<String> current = new HashSet<String>();
        while (matcher.find()) {
            String word = matcher.group().trim();
            char prefix = word.charAt(0);
            if (prefix == '-' || prefix == '+') {
                word = word.substring(1);
            }

            if (word.length() < 2 || STOP_WORDS.contains(word)) {
                continue;
            }

            if (prefix == '-') {
                query.unwanted.add(word);
                continue;
            }

            // TODO: 2016/10/9 last word:+hello current word:hello ?
            if (!current.isEmpty() || prefix != '+') {
                query.all.addAll(new ArrayList<String>(current));
                current.clear();
            }

            current.add(word);
        }

        if (!current.isEmpty()) {
            query.all.addAll(new ArrayList<String>(current));
        }

        return query;
    }

    /**
     * 对集合进行交集、并集、差集计算的辅助函数
     *
     * @param trans  传入的事务流水线
     * @param method 需要执行的集合操作
     * @param ttl    设置计算结果缓存的过期时间
     * @param items  操作的集合
     * @return 缓存的key
     */
    public String setCommon(Transaction trans, String method, int ttl, String... items) {
        String[] keys = new String[items.length];
        for (int i = 0; i < items.length; i++) {
            keys[i] = "idx:" + items[i];
        }
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


    /**
     * 对有序集合进行交集、并集的辅助函数
     *
     * @param trans  传入的事务流水线
     * @param method 需要执行的集合操作
     * @param ttl    设置计算结果缓存的过期时间
     * @param sets   操作的有序集合
     * @return 缓存的key
     */
    public String zSetCommon(Transaction trans, String method, ZParams zParams, int ttl, String... sets) {
        String[] keys = new String[sets.length];
        for (int i = 0; i < sets.length; i++) {
            keys[i] = "idx:" + sets[i];
        }
        String id = UUID.randomUUID().toString();
        try {
            trans.getClass().getDeclaredMethod(method, String.class, ZParams.class, sets.getClass()).invoke(trans, "idx:" + id, zParams, keys);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
        trans.expire("idx:" + id, ttl);
        return id;
    }


    public String zIntesect(Transaction trans, ZParams zParams, int ttl, String... sets) {
        return zSetCommon(trans, "zinterstore", zParams, ttl, sets);
    }

    public String zUnionStore(Transaction trans, ZParams zParams, int ttl, String... sets) {
        return zSetCommon(trans, "zunionstore", zParams, ttl, sets);
    }


    public class Query {
        public final List<String> all = new ArrayList<String>();
        public final Set<String> unwanted = new HashSet<String>();
    }


}
