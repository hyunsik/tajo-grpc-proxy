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

package org.diveintodata.gproxy;

import com.google.protobuf.ServiceException;
import io.grpc.ServerImpl;
import io.grpc.stub.StreamObserver;
import io.grpc.transport.netty.NettyServerBuilder;
import org.apache.tajo.TajoIdProtos.SessionIdProto;
import org.apache.tajo.ipc.ClientProtos.*;
import org.apache.tajo.ipc.TajoMasterClientProtocol;
import org.apache.tajo.ipc.TajoMasterClientProtocol.TajoMasterClientProtocolService.BlockingInterface;
import org.apache.tajo.ipc.TajoMasterClientProtocolServiceGrpc;
import org.apache.tajo.rpc.NettyClientBase;
import org.apache.tajo.rpc.RpcClientManager;
import org.apache.tajo.rpc.protocolrecords.PrimitiveProtos.BoolProto;
import org.apache.tajo.rpc.protocolrecords.PrimitiveProtos.KeyValueSetProto;
import org.apache.tajo.rpc.protocolrecords.PrimitiveProtos.StringListProto;
import org.apache.tajo.rpc.protocolrecords.PrimitiveProtos.StringProto;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.util.concurrent.TimeUnit;

public class ClientProxy implements TajoMasterClientProtocolServiceGrpc.TajoMasterClientProtocolService {
  private final InetSocketAddress serverAddr;
  private final InetSocketAddress lietenAddr;

  final RpcClientManager manager;
  private NettyClientBase client;
  private ServerImpl server;

  public ClientProxy(InetSocketAddress serverAddr, InetSocketAddress listenAddr) {
    this.serverAddr = serverAddr;
    this.lietenAddr = listenAddr;

    this.manager = RpcClientManager.getInstance();
  }

  public static String displayAddress(InetSocketAddress addr) {
    return addr.getAddress().getHostAddress() + ":" + addr.getPort();
  }

  public void start() throws NoSuchMethodException, IOException, ClassNotFoundException, InterruptedException {

    RpcClientManager.cleanup(client);
    this.client = manager.newClient(serverAddr, TajoMasterClientProtocol.class, false,
        manager.getRetries(), 0, TimeUnit.SECONDS, false);
    server = NettyServerBuilder.forAddress(lietenAddr).addService(
        TajoMasterClientProtocolServiceGrpc.bindService(this)
    ).build();
    server.start();
    System.out.println("Proxy starts up (" +
        displayAddress(lietenAddr) + "[listen] <=> " + displayAddress(serverAddr) + " [server])");

    Runtime.getRuntime().addShutdownHook(new Thread() {
      @Override
      public void run() {

        ClientProxy.this.client.close();

        // Use stderr here since the logger may have been reset by its JVM shutdown hook.
        System.err.println("* Shutting down gRPC server since JVM is shutting down");
        ClientProxy.this.stop();
        System.err.println("* server shut down");
      }
    });

    server.awaitTerminated();
  }

  private void stop() {
    if (server != null) {
      server.shutdown();
    }
  }

  @Override
  public void createSession(CreateSessionRequest request, StreamObserver<CreateSessionResponse> responseObserver) {
    try {
      BlockingInterface stub = client.getStub();
      responseObserver.onValue(stub.createSession(null, request));
      responseObserver.onCompleted();
    } catch (ServiceException e) {
      responseObserver.onError(e);
    }
  }

  @Override
  public void removeSession(SessionIdProto request, StreamObserver<BoolProto> responseObserver) {
    try {
      BlockingInterface stub = client.getStub();
      responseObserver.onValue(stub.removeSession(null, request));
      responseObserver.onCompleted();
    } catch (ServiceException e) {
      responseObserver.onError(e);
    }
  }

  @Override
  public void updateSessionVariables(UpdateSessionVariableRequest request, StreamObserver<SessionUpdateResponse> responseObserver) {
    try {
      BlockingInterface stub = client.getStub();
      responseObserver.onValue(stub.updateSessionVariables(null, request));
      responseObserver.onCompleted();
    } catch (ServiceException e) {
      responseObserver.onError(e);
    }
  }

  @Override
  public void existSessionVariable(SessionedStringProto request, StreamObserver<BoolProto> responseObserver) {
    try {
      BlockingInterface stub = client.getStub();
      responseObserver.onValue(stub.existSessionVariable(null, request));
      responseObserver.onCompleted();
    } catch (ServiceException e) {
      responseObserver.onError(e);
    }
  }

  @Override
  public void getSessionVariable(SessionedStringProto request, StreamObserver<StringProto> responseObserver) {
    try {
      BlockingInterface stub = client.getStub();
      responseObserver.onValue(stub.getSessionVariable(null, request));
      responseObserver.onCompleted();
    } catch (ServiceException e) {
      responseObserver.onError(e);
    }
  }

  @Override
  public void getAllSessionVariables(SessionIdProto request, StreamObserver<KeyValueSetProto> responseObserver) {
    try {
      BlockingInterface stub = client.getStub();
      responseObserver.onValue(stub.getAllSessionVariables(null, request));
      responseObserver.onCompleted();
    } catch (ServiceException e) {
      responseObserver.onError(e);
    }
  }

  @Override
  public void submitQuery(QueryRequest request, StreamObserver<SubmitQueryResponse> responseObserver) {
    try {
      BlockingInterface stub = client.getStub();
      responseObserver.onValue(stub.submitQuery(null, request));
      responseObserver.onCompleted();
    } catch (ServiceException e) {
      responseObserver.onError(e);
    }
  }

  @Override
  public void updateQuery(QueryRequest request, StreamObserver<UpdateQueryResponse> responseObserver) {
    try {
      BlockingInterface stub = client.getStub();
      responseObserver.onValue(stub.updateQuery(null, request));
      responseObserver.onCompleted();
    } catch (ServiceException e) {
      responseObserver.onError(e);
    }
  }

  @Override
  public void getQueryResult(GetQueryResultRequest request, StreamObserver<GetQueryResultResponse> responseObserver) {
    try {
      BlockingInterface stub = client.getStub();
      responseObserver.onValue(stub.getQueryResult(null, request));
      responseObserver.onCompleted();
    } catch (ServiceException e) {
      responseObserver.onError(e);
    }
  }

  @Override
  public void getQueryResultData(GetQueryResultDataRequest request, StreamObserver<GetQueryResultDataResponse> responseObserver) {
    try {
      BlockingInterface stub = client.getStub();
      responseObserver.onValue(stub.getQueryResultData(null, request));
      responseObserver.onCompleted();
    } catch (ServiceException e) {
      responseObserver.onError(e);
    }
  }

  @Override
  public void getQueryStatus(GetQueryStatusRequest request, StreamObserver<GetQueryStatusResponse> responseObserver) {
    try {
      BlockingInterface stub = client.getStub();
      responseObserver.onValue(stub.getQueryStatus(null, request));
      responseObserver.onCompleted();
    } catch (ServiceException e) {
      responseObserver.onError(e);
    }
  }

  @Override
  public void getRunningQueryList(GetQueryListRequest request, StreamObserver<GetQueryListResponse> responseObserver) {
    try {
      BlockingInterface stub = client.getStub();
      responseObserver.onValue(stub.getRunningQueryList(null, request));
      responseObserver.onCompleted();
    } catch (ServiceException e) {
      responseObserver.onError(e);
    }
  }

  @Override
  public void getFinishedQueryList(GetQueryListRequest request, StreamObserver<GetQueryListResponse> responseObserver) {
    try {
      BlockingInterface stub = client.getStub();
      responseObserver.onValue(stub.getFinishedQueryList(null, request));
      responseObserver.onCompleted();
    } catch (ServiceException e) {
      responseObserver.onError(e);
    }
  }

  @Override
  public void killQuery(QueryIdRequest request, StreamObserver<BoolProto> responseObserver) {
    try {
      BlockingInterface stub = client.getStub();
      responseObserver.onValue(stub.killQuery(null, request));
      responseObserver.onCompleted();
    } catch (ServiceException e) {
      responseObserver.onError(e);
    }
  }

  @Override
  public void getClusterInfo(GetClusterInfoRequest request, StreamObserver<GetClusterInfoResponse> responseObserver) {
    try {
      BlockingInterface stub = client.getStub();
      responseObserver.onValue(stub.getClusterInfo(null, request));
      responseObserver.onCompleted();
    } catch (ServiceException e) {
      responseObserver.onError(e);
    }
  }

  @Override
  public void closeNonForwardQuery(QueryIdRequest request, StreamObserver<BoolProto> responseObserver) {
    try {
      BlockingInterface stub = client.getStub();
      responseObserver.onValue(stub.closeNonForwardQuery(null, request));
      responseObserver.onCompleted();
    } catch (ServiceException e) {
      responseObserver.onError(e);
    }
  }

  @Override
  public void getQueryInfo(QueryIdRequest request, StreamObserver<GetQueryInfoResponse> responseObserver) {
    try {
      BlockingInterface stub = client.getStub();
      responseObserver.onValue(stub.getQueryInfo(null, request));
      responseObserver.onCompleted();
    } catch (ServiceException e) {
      responseObserver.onError(e);
    }
  }

  @Override
  public void createDatabase(SessionedStringProto request, StreamObserver<BoolProto> responseObserver) {
    try {
      BlockingInterface stub = client.getStub();
      responseObserver.onValue(stub.createDatabase(null, request));
      responseObserver.onCompleted();
    } catch (ServiceException e) {
      responseObserver.onError(e);
    }
  }

  @Override
  public void existDatabase(SessionedStringProto request, StreamObserver<BoolProto> responseObserver) {
    try {
      BlockingInterface stub = client.getStub();
      responseObserver.onValue(stub.existDatabase(null, request));
      responseObserver.onCompleted();
    } catch (ServiceException e) {
      responseObserver.onError(e);
    }
  }

  @Override
  public void dropDatabase(SessionedStringProto request, StreamObserver<BoolProto> responseObserver) {
    try {
      BlockingInterface stub = client.getStub();
      responseObserver.onValue(stub.dropDatabase(null, request));
      responseObserver.onCompleted();
    } catch (ServiceException e) {
      responseObserver.onError(e);
    }
  }

  @Override
  public void getAllDatabases(SessionIdProto request, StreamObserver<StringListProto> responseObserver) {
    try {
      BlockingInterface stub = client.getStub();
      responseObserver.onValue(stub.getAllDatabases(null, request));
      responseObserver.onCompleted();
    } catch (ServiceException e) {
      responseObserver.onError(e);
    }
  }

  @Override
  public void getCurrentDatabase(SessionIdProto request, StreamObserver<StringProto> responseObserver) {
    try {
      BlockingInterface stub = client.getStub();
      responseObserver.onValue(stub.getCurrentDatabase(null, request));
      responseObserver.onCompleted();
    } catch (ServiceException e) {
      responseObserver.onError(e);
    }
  }

  @Override
  public void selectDatabase(SessionedStringProto request, StreamObserver<BoolProto> responseObserver) {
    try {
      BlockingInterface stub = client.getStub();
      responseObserver.onValue(stub.selectDatabase(null, request));
      responseObserver.onCompleted();
    } catch (ServiceException e) {
      responseObserver.onError(e);
    }
  }

  @Override
  public void createExternalTable(CreateTableRequest request, StreamObserver<TableResponse> responseObserver) {
    try {
      BlockingInterface stub = client.getStub();
      responseObserver.onValue(stub.createExternalTable(null, request));
      responseObserver.onCompleted();
    } catch (ServiceException e) {
      responseObserver.onError(e);
    }
  }

  @Override
  public void existTable(SessionedStringProto request, StreamObserver<BoolProto> responseObserver) {
    try {
      BlockingInterface stub = client.getStub();
      responseObserver.onValue(stub.existDatabase(null, request));
      responseObserver.onCompleted();
    } catch (ServiceException e) {
      responseObserver.onError(e);
    }
  }

  @Override
  public void dropTable(DropTableRequest request, StreamObserver<BoolProto> responseObserver) {
    try {
      BlockingInterface stub = client.getStub();
      responseObserver.onValue(stub.dropTable(null, request));
      responseObserver.onCompleted();
    } catch (ServiceException e) {
      responseObserver.onError(e);
    }
  }

  @Override
  public void getTableList(GetTableListRequest request, StreamObserver<GetTableListResponse> responseObserver) {
    try {
      BlockingInterface stub = client.getStub();
      responseObserver.onValue(stub.getTableList(null, request));
      responseObserver.onCompleted();
    } catch (ServiceException e) {
      responseObserver.onError(e);
    }
  }

  @Override
  public void getTableDesc(GetTableDescRequest request, StreamObserver<TableResponse> responseObserver) {
    try {
      BlockingInterface stub = client.getStub();
      responseObserver.onValue(stub.getTableDesc(null, request));
      responseObserver.onCompleted();
    } catch (ServiceException e) {
      responseObserver.onError(e);
    }
  }

  @Override
  public void getFunctionList(SessionedStringProto request, StreamObserver<FunctionResponse> responseObserver) {
    try {
      BlockingInterface stub = client.getStub();
      responseObserver.onValue(stub.getFunctionList(null, request));
      responseObserver.onCompleted();
    } catch (ServiceException e) {
      responseObserver.onError(e);
    }
  }
}
