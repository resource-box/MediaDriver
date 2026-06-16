package com.resourcebox.AeronServer.service;

import com.resourcebox.MediaDriver.server.MediaDriverServer;
import jakarta.annotation.PostConstruct;
import org.springframework.stereotype.Service;

@Service
public class MediaDriverService {

    private static final String AERON_DIR = "PARCAeron";
    private static final int STREAM_ID = 10;

    @PostConstruct
    private void run() {
        MediaDriverServer server = new MediaDriverServer("PARCAeron", 1, 10, 5);
        server.start();
    }

}
