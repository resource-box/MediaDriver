package com.resourcebox.AeronServer.service;

import com.resourcebox.MediaDriver.server.MediaDriverServer;
import jakarta.annotation.PostConstruct;
import org.springframework.stereotype.Service;

@Service
public class MediaDriverService {

    private static final String AERON_DIR = "PARCAeron";
    private static final int IPC_TERM_BUFFER_MB = 32;
    private static final int SOCKET_BUFFER_MB = 2;

    @PostConstruct
    private void run() {
        MediaDriverServer server = new MediaDriverServer(AERON_DIR, IPC_TERM_BUFFER_MB, SOCKET_BUFFER_MB);
        server.start();
    }

}
