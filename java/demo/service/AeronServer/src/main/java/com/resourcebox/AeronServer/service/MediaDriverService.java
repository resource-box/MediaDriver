package com.resourcebox.AeronServer.service;

import com.resourcebox.MediaDriver.server.MediaDriverServer;
import jakarta.annotation.PostConstruct;
import org.springframework.stereotype.Service;

@Service
public class MediaDriverService {

    private static final String AERON_DIR = "PARCAeron";

    @PostConstruct
    private void run() {
        MediaDriverServer server = new MediaDriverServer(AERON_DIR);
        server.start();
    }

}
