package com.isaolmez.kafkaguide.vanilla.admin.lesson4;

public class Runner {

    public static void main(String[] args) {
        final AdminClientService adminClientService = new AdminClientService();
        adminClientService.describeTopic();
    }
}
