package aoptest;

import daily.aopjava.service.Father;
import org.junit.Test;
import org.springframework.context.ApplicationContext;
import org.springframework.context.support.ClassPathXmlApplicationContext;

/**
 * Created by adimn on 2019/4/16.
 */
public class AopTest {

    @Test
    public void testAop(){
        ApplicationContext context = new ClassPathXmlApplicationContext("classpath:applicationContext.xml");
        Father father = (Father) context.getBean("father");
        father.sayName();
        father.sleep();
    }
}
