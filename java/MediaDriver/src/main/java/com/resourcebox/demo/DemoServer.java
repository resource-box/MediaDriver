package com.resourcebox.demo;

import com.resourcebox.MediaDriver.server.MediaDriverServer;

public class DemoServer {
    public static void main(String[] args) {
        System.out.println("Starting Demo Media Driver Server...");
        MediaDriverServer server = new MediaDriverServer("PARCAeron", 32, 2);
        server.start();
    }
}
