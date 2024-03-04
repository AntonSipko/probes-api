package telran.probes;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.mockito.Mockito.when;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.mock.mockito.MockBean;
import org.springframework.cloud.stream.binder.test.InputDestination;
import org.springframework.cloud.stream.binder.test.OutputDestination;
import org.springframework.cloud.stream.binder.test.TestChannelBinderConfiguration;
import org.springframework.context.annotation.Import;
import org.springframework.messaging.Message;
import org.springframework.messaging.support.GenericMessage;

import com.fasterxml.jackson.databind.ObjectMapper;

import telran.probes.dto.DeviationData;
import telran.probes.dto.ProbeData;
import telran.probes.dto.Range;
import telran.probes.service.RangeProviderClientService;
@SpringBootTest
@Import(TestChannelBinderConfiguration.class)
class AnalyzerControllerTest {
	private static final long SENSOR_ID = 123;
	private static final  double MIN_VALUE = 100;
	private static final  double MAX_VALUE = 200;
	private static final double VALUE_LESS_MIN = 50;
	private static final double VALUE_GREATER_MAX = 220;
	private static final Range RANGE = new Range(MIN_VALUE, MAX_VALUE);
	private static final double VALUE_NORMAL= 150;
	private static final Double DEVIATION_GREATER_MAX = VALUE_GREATER_MAX - MAX_VALUE;
	private static final Double DEVIATION_LESS_MIN = VALUE_LESS_MIN - MIN_VALUE;
	@MockBean
RangeProviderClientService clientService;
	@Autowired
	InputDestination producer;
	@Autowired
	OutputDestination consumer;
	private ProbeData probeNormalData =  new ProbeData(SENSOR_ID, VALUE_NORMAL, System.currentTimeMillis());
	private String consumerBindingName = "analyzerConsumer-in-0";
	private String producerBindingName = "analyzerProducer-out-0";
	private ProbeData probeGreaterMaxData = new ProbeData(SENSOR_ID, VALUE_GREATER_MAX, System.currentTimeMillis());
	ObjectMapper mapper = new ObjectMapper();
	private ProbeData probeLessMinData = new ProbeData(SENSOR_ID, VALUE_LESS_MIN, System.currentTimeMillis());
	@Test
	void loadApplicationContext() {
		assertNotNull(clientService);
	}
	@BeforeEach
	void setUp() {
		when(clientService.getRange(SENSOR_ID)).thenReturn(RANGE);
		
	}
	@Test
	void noDeviationTest() {
		producer.send(new GenericMessage<ProbeData>(probeNormalData), consumerBindingName);
		Message<byte[]> message = consumer.receive(10, producerBindingName);
		assertNull(message);
	}
	@Test
	void greaterMaxDeviationTest() throws Exception {
		producer.send(new GenericMessage<ProbeData>(probeGreaterMaxData), consumerBindingName);
		Message<byte[]> message = consumer.receive(10, producerBindingName);
		assertNotNull(message);
		DeviationData deviationData = mapper.readValue(message.getPayload(), DeviationData.class);
		assertEquals(SENSOR_ID, deviationData.id());
		assertEquals(DEVIATION_GREATER_MAX, deviationData.deviation());
		
	}
	@Test
	void lessMinDeviationTest() throws Exception {
		producer.send(new GenericMessage<ProbeData>(probeLessMinData), consumerBindingName);
		Message<byte[]> message = consumer.receive(10, producerBindingName);
		assertNotNull(message);
		DeviationData deviationData = mapper.readValue(message.getPayload(), DeviationData.class);
		assertEquals(SENSOR_ID, deviationData.id());
		assertEquals(DEVIATION_LESS_MIN, deviationData.deviation());
		
	}
	

}
