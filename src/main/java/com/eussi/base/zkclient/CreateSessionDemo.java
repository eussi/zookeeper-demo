package com.eussi.base.zkclient;

import org.I0Itec.zkclient.ZkClient;

import java.io.IOException;

/**
 * Created by wangxueming on 2018/9/29.
 */
public class CreateSessionDemo {
    public final static String CONNECTIONSTRING =  "192.168.198.201:2181," +
            "192.168.198.202:2181," +
            "192.168.198.203:2181," +
            "192.168.198.204:2181";

    public static void main(String[] args) throws IOException, InterruptedException {
        ZkClient zkClient = new ZkClient(CONNECTIONSTRING, 5000);

        System.out.println("connected->" + zkClient);

    }
}
