package telran.probes.service;

import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.web.client.RestTemplate;

import lombok.Getter;

@Configuration
@Getter
public class ServiceConfiguration {
@Value("${app.range.provider.host:localhost}")
	String host;
@Value("${app.range.provider.port:8282}")
int port;
@Value("${app.range.provider.path:/range/sensor/}")
String path;
@Bean
RestTemplate getRestTemplate() {
	return new RestTemplate();
}
}
