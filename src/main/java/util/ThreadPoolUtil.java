package util;


import java.util.concurrent.LinkedBlockingDeque;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

/**
 * @Author: PJ, SATAN LOVES YOU FOREVER
 * @Date: 2023/11/15 14:13
 * @Function:
 */
public class ThreadPoolUtil {
    private static final ThreadPoolExecutor pool;

    static {
        //这些参数有什么好的设置方法？
        //pool一旦被new出来就不会再变了，所以这些参数应该是final的？
        pool = new ThreadPoolExecutor(
                10,
                20,
                60,
                TimeUnit.MINUTES,
                new LinkedBlockingDeque<>()
        );
    }

    public static ThreadPoolExecutor getThreadPoolExecutor(){
        return pool;
    }

    public static void main(String[] args) {
        ThreadPoolExecutor poolExecutor = ThreadPoolUtil.getThreadPoolExecutor();
        for (int i = 0; i < 15; i++) {
            poolExecutor.execute(new Runnable() {
                @Override
                public void run() {
                    System.out.println(Thread.currentThread().getName());
                }
            });
        }
    }
}
