import com.fasterxml.jackson.core.JsonFactory;
import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.junit.Test;

import java.io.Serializable;
import java.util.Date;

/**
 * author:xszhaobo
 * <p/>
 * date:2016/9/11
 * <p/>
 * package_name:PACKAGE_NAME
 * <p/>
 * project: my-redis-in-action
 */
public class JsonTest {

    @Test
    public void jsonTest() throws JsonProcessingException {
        ObjectMapper objectMapper = new ObjectMapper();
        Aoo aoo = new Aoo();
        aoo.setName("aoo");
        aoo.setAge(5);
        aoo.setAddress("杭州");
        aoo.setBirthday(new Date());
        String s = objectMapper.writeValueAsString(aoo);
        System.out.println(s);
    }


    private class Aoo implements Serializable {
        private String name;
        private int age;
        private Date birthday;
        private String address;

        public String getName() {
            return name;
        }

        public void setName(String name) {
            this.name = name;
        }

        public int getAge() {
            return age;
        }

        public void setAge(int age) {
            this.age = age;
        }

        public Date getBirthday() {
            return birthday;
        }

        public void setBirthday(Date birthday) {
            this.birthday = birthday;
        }

        public String getAddress() {
            return address;
        }

        public void setAddress(String address) {
            this.address = address;
        }
    }
}
