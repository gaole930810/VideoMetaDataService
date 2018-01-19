package com.Test;

import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.util.concurrent.CountDownLatch;

import org.apache.commons.lang.time.StopWatch;

import com.VMDServiceClientAPI.ClientAPI;


public class MultiThreadTest{
	public static void main(String[]args) throws InterruptedException{
		//并行度10000
        int parallel = Integer.valueOf(args[0]);

        //开始计时
        StopWatch sw = new StopWatch();
        sw.start();

        CountDownLatch signal = new CountDownLatch(1);
        CountDownLatch finish = new CountDownLatch(parallel);
        for (int i = 0; i < parallel; i++) {
        	final int index=i;
            new Thread(
            		new Runnable(){
            			public void run(){
            				try {
            					signal.await();
            					if(args.length==4)
            						VMDServerTest(signal,finish,index,args[1],args[2],args[3]);
            					if(args.length==3)
    								VMDServerTest(signal,finish,index,args[1],args[2]);
								finish.countDown();
							} catch (ClassNotFoundException e) {
								// TODO Auto-generated catch block
								e.printStackTrace();
							} catch (NoSuchMethodException e) {
								// TODO Auto-generated catch block
								e.printStackTrace();
							} catch (SecurityException e) {
								// TODO Auto-generated catch block
								e.printStackTrace();
							} catch (IllegalAccessException e) {
								// TODO Auto-generated catch block
								e.printStackTrace();
							} catch (IllegalArgumentException e) {
								// TODO Auto-generated catch block
								e.printStackTrace();
							} catch (InvocationTargetException e) {
								// TODO Auto-generated catch block
								e.printStackTrace();
							} catch (InterruptedException e) {
								// TODO Auto-generated catch block
								e.printStackTrace();
							}
            			}
            		}
            ).start();
        }
        
        
      //10000个并发线程瞬间发起请求操作
        signal.countDown();
        finish.await();
        
        sw.stop();
        String tip = String.format("RPC调用总共耗时: [%s] 毫秒", sw.getTime());
        System.out.println(tip);
	}
	/*Results LS(String url)
	 *Results GET(String url) 
	 *Results GET_FRAME(String url,String FrameSeq)
	 *Results GENERATE(String url)
	 *Results DELETE(String url) 
	*/
	public static void VMDServerTest(CountDownLatch signal, CountDownLatch finish,int index,String commandType,String... commandArgs) throws ClassNotFoundException, NoSuchMethodException, SecurityException, IllegalAccessException, IllegalArgumentException, InvocationTargetException{
		switch(commandType){
		case "LS":
			ClientAPI.VMDLS(commandArgs[0]).print();
			break;
		case "GET":
			ClientAPI.VMDGET(commandArgs[0]).print();
			break;
		case "GETFRAME":
			ClientAPI.GET_FRAME_Index_By_FrameNo(commandArgs[0], commandArgs[1]).print();
			break;
		case "GENERATE":
			ClientAPI.VMDGENERATE(commandArgs[0]).print();
			break;
		case "DELETE":
			ClientAPI.VMDDELETE(commandArgs[0]).print();
			break;
		default:
			break;
		}
		
		/*ClassLoader classLoader=MultiThreadTest.class.getClassLoader();
		Class<?> cTest=classLoader.loadClass("com.VMDServiceAPI.VMDClient");
		Method method=cTest.getMethod(commandType, String[].class);
		if(commandArgs.length==1){
			Results re=(Results)method.invoke(null, new Object[]{new String[]{commandArgs[0]}});
			System.out.println(re);
		}else if(commandArgs.length==2){
			Results re=(Results)method.invoke(null, new Object[]{new String[]{commandArgs[0],commandArgs[1]}});
			System.out.println(re);
		}*/
	}
}