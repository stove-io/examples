package com.trendyol.stove.examples.ecomm.java.orders;

import io.grpc.Server;
import io.grpc.ServerBuilder;
import java.io.IOException;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

@Configuration
class OrderQueryServer {
  @Bean(destroyMethod = "shutdown")
  Server orderGrpcServer(OrderQueryService service, @Value("${order.grpc.port:9091}") int port) throws IOException {
    return ServerBuilder.forPort(port).addService(service).build().start();
  }
}
