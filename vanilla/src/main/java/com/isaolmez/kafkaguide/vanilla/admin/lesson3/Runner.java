package com.isaolmez.kafkaguide.vanilla.admin.lesson3;

public class Runner {

    public static void main(String[] args) {
        final AdminClientService adminClientService = new AdminClientService();
//        adminClientService.describeCluster();
        adminClientService.describeClusterWithOptions();
    }
}
