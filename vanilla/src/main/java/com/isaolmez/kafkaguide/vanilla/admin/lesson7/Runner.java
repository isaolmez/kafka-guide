package com.isaolmez.kafkaguide.vanilla.admin.lesson7;

public class Runner {

    public static void main(String[] args) {
        final AdminClientService adminClientService = new AdminClientService();
//        adminClientService.listOffsets();
        adminClientService.alterConsumerOffsets();
    }
}
