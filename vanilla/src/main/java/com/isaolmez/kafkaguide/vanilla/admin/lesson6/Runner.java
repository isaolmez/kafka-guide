package com.isaolmez.kafkaguide.vanilla.admin.lesson6;

public class Runner {

    public static void main(String[] args) {
        final AdminClientService adminClientService = new AdminClientService();
//        adminClientService.listConsumerGroups();
//        adminClientService.listConsumerGroupsWithValid();
//        adminClientService.describeConsumerGroups();
        adminClientService.listConsumerGroupOffsets();
    }
}
