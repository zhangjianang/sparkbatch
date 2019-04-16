package daily.aopjava.service;

import org.springframework.stereotype.Component;

/**
 * Created by adimn on 2019/4/16.
 */
@Component
public class Father implements Person {
    @Override
    public void sayName() {
        System.out.println("i am father" );
    }

    public void sleep(){
        System.out.println("i like sleep");
    }
}
