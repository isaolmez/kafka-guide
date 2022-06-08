package com.isaolmez.kafkaguide.vanilla.admin.lesson5;

public class Runner {

    public static void main(String[] args) {
        final AdminClientService adminClientService = new AdminClientService();
//        adminClientService.describeConfig();
        adminClientService.alterConfig();
    }
}
