package com.isaolmez.kafkaguide.vanilla.admin.lesson2;

public class Runner {

    public static void main(String[] args) {
        final AdminClientService adminClientService = new AdminClientService();
//        adminClientService.createTopic();
//        adminClientService.deleteTopic();
        adminClientService.deleteTopicWithOptions();
    }
}
