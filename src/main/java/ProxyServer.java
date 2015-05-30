import org.apache.commons.cli.*;

import java.io.IOException;
import java.net.InetSocketAddress;

/**
 * Copyright 2015 Hyunsik Choi (hyunsik.choi@gmail.com) All Rights Reserved
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

public class ProxyServer {
  private static ClientProxy clientProxy;

  public static void run(String [] args) throws Throwable {
    if (args.length < 2) {
      System.err.println("usage: ./tajo-grpc-proxy [server address] [listen address]");
      System.exit(-1);
    }

    InetSocketAddress serverAddr = null;
    InetSocketAddress listenAddr = null;

    try {
      serverAddr = createSocketAddr(args[0]);
    } catch (Throwable t) {
      System.err.println("Invalid server address: " + args[0]);
      System.exit(-2);
    }

    try {
      listenAddr = createSocketAddr(args[1]);
    } catch (Throwable t) {
      System.err.println("Invalid listen address: " + args[0]);
      System.exit(-3);
    }

    clientProxy = new ClientProxy(serverAddr, listenAddr);
    clientProxy.start();
  }

  public static InetSocketAddress createSocketAddr(String addr) {
    String [] splitted = addr.split(":");
    return new InetSocketAddress(splitted[0], Integer.parseInt(splitted[1]));
  }

  public static void main(String [] args) throws Throwable {
    run(args);
  }
}
