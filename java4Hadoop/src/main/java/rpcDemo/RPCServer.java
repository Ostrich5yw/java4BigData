package rpcDemo;

import lombok.extern.slf4j.Slf4j;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.ipc.RPC;
import org.apache.hadoop.ipc.Server;

import java.io.IOException;

@Slf4j
public class RPCServer implements RPCProtocol{
    public static void main(String[] args) throws IOException {
        Server server = new RPC.Builder(new Configuration())
                .setBindAddress("localhost")
                .setPort(8888)
                .setProtocol(RPCProtocol.class)
                .setInstance(new RPCServer())
                .build();
        log.info("服务端启动");
        server.start();
    }

    @Override
    public void dosth() {
        log.info("服务端dosth()被调用");
    }
}
