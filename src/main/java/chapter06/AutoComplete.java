package chapter06;

import org.junit.Test;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.Transaction;

import java.util.ArrayList;
import java.util.List;
import java.util.Random;

/**
 * author:xszhaobo
 * <p/>
 * date:2016/9/12
 * <p/>
 * package_name:chapter06
 * <p/>
 * project: my-redis-in-action
 * 在Web领域里，自动补全(autocomplete)是一种能够让
 * 用户在不进行搜索的情况下，快速找到所需要东西的技术。
 * <p/>
 * 创建一个常用联系人列表，保存每个用户最近联系的100个用户。
 * 使用列表是因为列表占用的内存是最少的。
 */
public class AutoComplete {


    @Test
    public void testAutoComplete() {
        Jedis conn = new Jedis("localhost");
        conn.select(0);
        List<String> list = new ArrayList<String>();
        list.add("jack");
        list.add("lily");
        list.add("lucy");
        list.add("jim");
        list.add("tom");
        list.add("java");
        list.add("c");
        list.add("python");
        list.add("jack");
        Random random = new Random();
        for (int i = 0; i < 500; i++) {
            String contact = list.get(random.nextInt(9));
            if (i == 450) {
                contact = "jack";
            }
            addAndUpdateContact(conn,"bo.zhao",contact + i);
        }
        delContact(conn,"bo.zhao","jack" + 450);
        List<String> contactList = fetchAutocompleteList(conn,"bo.zhao","t");
        System.out.println(contactList);
    }

    /**
     * 操作就是添加或更新一个联系人，让他成为最新的被
     * 联系用户。
     * 1.如果指定的联系人已经存在于最近联系人列表，那么删除该联系人；
     * 2.把该联系人添加到最近联系人的最前面；
     * 3.修剪最近联系人列表，让它只包含100个最近联系人。
     *
     * @param conn    redis连接
     * @param user    用户
     * @param contact 最新联系人
     */
    private void addAndUpdateContact(Jedis conn, String user, String contact) {
        String key = "recent:" + user;
        Transaction trans = conn.multi();
        trans.lrem(key, 1, contact);
        trans.lpush(key, contact);
        trans.ltrim(key, 0, 99);
        trans.exec();
    }

    /**
     * 从最近联系人中删除给定的联系人
     *
     * @param conn    redis连接
     * @param user    用户
     * @param contact 联系人
     */
    private void delContact(Jedis conn, String user, String contact) {
        String key = "recent:" + user;
        conn.lrem(key, 0, contact);
    }

    /**
     * 获取自动补全列表并查找以<code>prefix</code>开头的联系人
     *
     * @param conn   redis连接
     * @param user   用户
     * @param prefix 联系人
     * @return 匹配的联系人列表
     */
    private List<String> fetchAutocompleteList(Jedis conn, String user, String prefix) {
        String key = "recent:" + user;
        List<String> result = new ArrayList<String>();
        List<String> contactList = conn.lrange(key, 0, -1);
        for (String s : contactList) {
            if (s.startsWith(prefix)) {
                result.add(s);
            }
        }
        return result;
    }
}
