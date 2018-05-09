package com.carpo.spark.stream;

import java.io.IOException;
import java.io.OutputStream;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.Random;

/**
 * Created by lg on 2018/5/8.
 */
public class SocketServerMain {
    public static void main(String[] args) throws Exception {
        Socket socket;

        ServerSocket server = new ServerSocket(9999);
        while (true) try {
            socket = server.accept();
            System.out.println(socket.getInetAddress().toString());
            OutputStream os = socket.getOutputStream();
            for (int i = 0; i < 10; i++) {
                os.write(String.valueOf((char) (new Random().nextInt(20) + 97)+"\n").getBytes());
                os.flush();
            }
            os.close();

            socket.close();
        } catch (IOException e) {
            e.printStackTrace();
        } finally {
        }

    }
}
