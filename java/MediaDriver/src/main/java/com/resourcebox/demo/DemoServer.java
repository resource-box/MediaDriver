package com.resourcebox.demo;

import com.resourcebox.MediaDriver.server.MediaDriverServer;

public class DemoServer {
    public static void main(String[] args) {
        System.out.println("Starting Demo Media Driver Server...");
        // PARCAeron 디렉토리를 사용하며, Sender=1, Receiver=4, Conductor=7 코어 할당
        MediaDriverServer server = new MediaDriverServer("PARCAeron", 1, 10, 5);
        server.start();
    }
}
