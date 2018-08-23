package com.spf.utils.executor;

import java.util.concurrent.LinkedBlockingDeque;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

/**
 * @author ShuPF
 * @类说明： 线程池
 * @date 2018-08-23 11:22
 */
public class ThreadPoolUtil {

    /**
     * 从执行结果可以看出，当线程池中线程的数目大于5时，便将任务放入任务缓存队列里面，
     * 当任务缓存队列满了之后，便创建新的线程。如果上面程序中，将for循环中改成执行20个任务，就会抛出任务拒绝异常了。
     * @param args
     */
    public static void main(String[] args) {
            ThreadPoolExecutor executor = new ThreadPoolExecutor(5, 10, 200, TimeUnit.MILLISECONDS,
                    new LinkedBlockingDeque<>());

            for(int i=0;i<20;i++){
                MyTask myTask = new MyTask(i);
                executor.execute(myTask);
                System.out.println("线程池中线程数目："+executor.getPoolSize()+"，队列中等待执行的任务数目："+
                        executor.getQueue().size()+"，已执行玩别的任务数目："+executor.getCompletedTaskCount());
            }
            executor.shutdown();
        }
}


    class MyTask implements Runnable {
        private int taskNum;

        public MyTask(int num) {
            this.taskNum = num;
        }

        @Override
        public void run() {
            System.out.println("正在执行task "+taskNum);
            try {
                Thread.currentThread().sleep(1000);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
            System.out.println("task "+taskNum+"执行完毕");
        }
}
