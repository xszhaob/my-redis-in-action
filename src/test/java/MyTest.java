/**
 * author:xszhaobo
 * <p/>
 * date:2016/10/30
 * <p/>
 * package_name:PACKAGE_NAME
 * <p/>
 * project: my-redis-in-action
 */
public class MyTest {
    public static void main(String[] args) {
        String member = 1 + "&&" + 2 + "&&" + 3 + "&&" + 4;
        System.out.println(member.substring(member.lastIndexOf("&&") + 2));
    }
}
