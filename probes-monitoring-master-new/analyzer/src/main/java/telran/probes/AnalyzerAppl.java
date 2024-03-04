package telran.probes;

import java.util.HashMap;
import java.util.Map;
import java.util.function.Consumer;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.cloud.stream.function.StreamBridge;
import org.springframework.context.annotation.Bean;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;

import telran.probes.dto.*;
import telran.probes.service.RangeProviderClientService;

@SpringBootApplication
@RequiredArgsConstructor
@Slf4j
public class AnalyzerAppl {
    String producerBindingName = "analyzerProducer-out-0";
    final RangeProviderClientService clientService;
    final StreamBridge streamBridge;
    private final Map<Long, Range> sensorIdToRangeMap = new HashMap<>();
    private String updateBindingName = "updateRangeConsumer-in-0";

    public static void main(String[] args) {
        SpringApplication.run(AnalyzerAppl.class, args);

    }

    @Bean
    Consumer<ProbeData> analyzerConsumer() {
        return probeData -> probeDataAnalyzing(probeData);
    }

    @Bean
    Consumer<SensorUpdateData> updateRangeConsumer() {
        return this::updateProcessing;
    }

    private void probeDataAnalyzing(ProbeData probeData) {
        log.trace("received probe: {}", probeData);
        long sensorId = probeData.id();
        Range range = clientService.getRange(sensorId);
        double value = probeData.value();

        double border = Double.NaN;
        if (value < range.minValue()) {
            border = range.minValue();
        } else if (value > range.maxValue()) {
            border = range.maxValue();
        }
        if (!Double.isNaN(border)) {
            double deviation = value - border;
            log.debug("deviation: {}", deviation);
            DeviationData dataDeviation =
                    new DeviationData(sensorId, deviation, value, System.currentTimeMillis());
            streamBridge.send(producerBindingName, dataDeviation);
            log.debug("deviation data {} sent to {}", dataDeviation, producerBindingName);

        }
    }

    private void updateProcessing(SensorUpdateData sensorUpdateData) {
        Long sensorId = sensorUpdateData.id();
        Range sensorUpdatedRange = sensorUpdateData.range();
        if (sensorUpdatedRange != null) {
            sensorIdToRangeMap.put(sensorId, sensorUpdatedRange);
            log.debug("Updated Range for Sensor ID {}: {}", sensorId, sensorUpdatedRange);
            streamBridge.send(updateBindingName, sensorUpdateData);
        } else {
            log.warn("Ignoring SensorUpdateData: {}", sensorUpdateData);
        }

    }

}
