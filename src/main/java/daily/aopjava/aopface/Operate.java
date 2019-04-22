package daily.aopjava.aopface;


import org.aspectj.lang.ProceedingJoinPoint;
import org.aspectj.lang.annotation.Aspect;
import org.aspectj.lang.annotation.Before;
import org.aspectj.lang.annotation.Pointcut;
import org.springframework.stereotype.Component;

/**
 * Created by adimn on 2019/4/10.
 */
@Component
@Aspect
public class Operate {

//    @Pointcut("execution( * daily.aopjava.service.Father.sayName(..))")
//    public void pointcut(){};

    @Pointcut("this(daily.aopjava.service.Person+)")
    public void pointcut(){};

//    @Around("pointcut()")
//    public Object aroundAdvice(ProceedingJoinPoint joinPoint) {
//        String className = joinPoint.getThis().toString();
//        String methodName = joinPoint.getSignature().getName();
//        long st = System.currentTimeMillis();
//        try {
//            Object result = joinPoint.proceed();
//            long elapse = System.currentTimeMillis() - st;
//            System.out.println(methodName +",类名："+className +",持续时间："+elapse);
//            return result;
//        } catch (Throwable throwable) {
//            throwable.printStackTrace();
//        }
//        return null;
//    }

    @Before("pointcut()")
    public void beforeAdvice(){
        System.out.println("i am before");
    }
}
