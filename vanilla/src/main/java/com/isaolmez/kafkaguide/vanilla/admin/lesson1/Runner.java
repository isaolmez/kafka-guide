package com.isaolmez.kafkaguide.vanilla.admin.lesson1;

public class Runner {

    public static void main(String[] args) {
        final AdminClientService adminClientService = new AdminClientService();
//        adminClientService.listTopicsWithNames();
//        adminClientService.listTopicsWithDetails();
        adminClientService.listTopicsWithOptions();
    }
}
