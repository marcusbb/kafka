package org.marcusbb.queue.kafka.utils;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.net.ServerSocket;
import java.util.Random;

public class TestUtils {
    private static final Random RANDOM = new Random();

    private TestUtils() {
    }

    public static File constructTempDir(String dirPrefix) {
        File file = new File(System.getProperty("java.io.tmpdir"), dirPrefix + RANDOM.nextInt(10000000));
        if (!file.mkdirs()) {
            throw new RuntimeException("could not create temp directory: " + file.getAbsolutePath());
        }
        file.deleteOnExit();
        return file;
    }

    public static int getAvailablePort() {
    	try {
    		new ServerSocket(9092).close();
    		return 9092;
    	}catch (Exception e) {
    		System.out.println("Can't establish default port 9092");
    	}
        try {
        	
            ServerSocket socket = new ServerSocket(0);
            try {
                return socket.getLocalPort();
            } finally {
                socket.close();
            }
        } catch (IOException e) {
            throw new IllegalStateException("Cannot find available port: " + e.getMessage(), e);
        }
    }

    public static boolean deleteFile(File path) throws FileNotFoundException {
        if (!path.exists()) {
            throw new FileNotFoundException(path.getAbsolutePath());
        }
        boolean ret = true;
        if (path.isDirectory()) {
            for (File f : path.listFiles()) {
                ret = ret && deleteFile(f);
            }
        }
        return ret && path.delete();
    }

	public static byte []randomBytes(int size) {
		byte []b_input = new byte[size];
		Random r = new Random();
		int bi = 0;
		for (int i=0;i<size;i++) {
			b_input[i] = (byte)r.nextInt(256);
		}
		return b_input;
	}

    public static int resolvePort(int port) {
        if (port == -1) {
            return getAvailablePort();
        }
        return port;
    }
}
