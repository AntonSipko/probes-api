package telran.probes;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.when;

import telran.probes.dto.*;
import org.junit.jupiter.api.*;
import org.mockito.invocation.InvocationOnMock;
import org.mockito.stubbing.Answer;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.mock.mockito.MockBean;
import org.springframework.cloud.stream.binder.test.*;
import org.springframework.context.annotation.Import;
import org.springframework.http.HttpMethod;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.messaging.support.GenericMessage;
import org.springframework.web.client.RestTemplate;

import telran.probes.service.RangeProviderClientService;

@SpringBootTest
@Import(TestChannelBinderConfiguration.class)
@TestMethodOrder(MethodOrderer.OrderAnnotation.class)
class AnalyzerServiceTest {
	private static final long SENSOR_ID = 123;
	private static final double MIN_VALUE = 100;
	private static final double MAX_VALUE = 200;
	private static final Range RANGE = new Range(MIN_VALUE, MAX_VALUE);
	private static final String URL = "http://localhost:8282/range/sensor/";
	private static final String SENSOR_NOT_FOUND_MESSAGE ="sensor not found";
	private static final long SENSOR_ID_NOT_FOUND = 124;
	private static final Range RANGE_DEFAULT =
			new Range(RangeProviderClientService.MIN_DEFAULT_VALUE, RangeProviderClientService.MAX_DEFAULT_VALUE);
	private static final long SENSOR_ID_UNAVAILABLE = 170;
	private static final Range RANGE_UPDATED = new Range(MIN_VALUE + 10, MAX_VALUE + 10);
	@Autowired
	InputDestination producer;
	@Autowired
	RangeProviderClientService providerService;
	@MockBean
	RestTemplate restTemplate;
	private String updateBindingName = "updateRangeConsumer-in-0";

	@Test
	@Order(1)
	void normalFlowNoCache() {
		ResponseEntity<Range> responseEntity = new ResponseEntity<>(RANGE,HttpStatus.OK);
		when(restTemplate.exchange(getUrl(SENSOR_ID), HttpMethod.GET, null, Range.class))
		.thenReturn(responseEntity );
		assertEquals(RANGE, providerService.getRange(SENSOR_ID));
	}
	@SuppressWarnings("unchecked")
	@Order(2)
	void normalFlowWithCache() {
		when(restTemplate.exchange(anyString(), any(HttpMethod.class), any(), any(Class.class)))
		.thenAnswer(new Answer<ResponseEntity<?>>() {
			@Override
			public ResponseEntity<?> answer(InvocationOnMock invocation) throws Throwable {
				fail("method exchange should not be called");
				return null;
			}
		});
		assertEquals(RANGE, providerService.getRange(SENSOR_ID));
	}
	@Test
	@Order(3)
	void sensorNotFoundTest() {
		ResponseEntity<String> responseEntity = new ResponseEntity<>(SENSOR_NOT_FOUND_MESSAGE,HttpStatus.NOT_FOUND);
		when(restTemplate.exchange(getUrl(SENSOR_ID_NOT_FOUND), HttpMethod.GET, null, String.class))
		.thenReturn(responseEntity );
		assertEquals(RANGE_DEFAULT, providerService.getRange(SENSOR_ID_NOT_FOUND));
		
	}
	@Test
	@Order(4)
	void defaultRangeNotInCache() {
		ResponseEntity<Range> responseEntity = new ResponseEntity<>(RANGE_DEFAULT,HttpStatus.OK);
		when(restTemplate.exchange(getUrl(SENSOR_ID_NOT_FOUND), HttpMethod.GET, null, Range.class))
		.thenReturn(responseEntity );
		assertEquals(RANGE_DEFAULT, providerService.getRange(SENSOR_ID_NOT_FOUND));
	}
	@SuppressWarnings("unchecked")
	@Test
	void remoteWebServiceUnavailable() {
		when(restTemplate.exchange(anyString(), any(HttpMethod.class), any(), any(Class.class)))
        .thenThrow(new RuntimeException("Service is unavailable"));
assertEquals(RANGE_DEFAULT, providerService.getRange(SENSOR_ID_UNAVAILABLE));
}
	@Test
	void updateRangeSensorInMap() throws InterruptedException {
		
		producer.send(new GenericMessage<SensorUpdateData>(new SensorUpdateData(SENSOR_ID,
				RANGE_UPDATED, null)), updateBindingName);
		Thread.sleep(100);
		assertEquals(RANGE, providerService.getRange(SENSOR_ID));
	}
	

	private String getUrl(long sensorId) {
		
		return URL + sensorId;
	}

}
