package telran.probes.service;

import java.util.HashMap;
import java.util.Map;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.HttpMethod;
import org.springframework.http.ResponseEntity;
import org.springframework.stereotype.Service;
import org.springframework.web.client.RestTemplate;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import telran.probes.dto.Range;

@Service
@RequiredArgsConstructor
@Slf4j
public class RangeProviderClientServiceImpl implements RangeProviderClientService {

    private final RestTemplate restTemplate;
    private final ServiceConfiguration serviceConfiguration;
    private final Map<Long, Range> rangeCache = new HashMap<>();
    private static final Range RANGE_DEFAULT = new Range(RangeProviderClientService.MIN_DEFAULT_VALUE, RangeProviderClientService.MAX_DEFAULT_VALUE);

    @Override
    public Range getRange(long sensorId) {
        Range range = rangeCache.get(sensorId);
        if (range == null) {
            range = serviceRequest(sensorId);
            if (range != null) {
                rangeCache.put(sensorId, range);
            }
        }
        return range;
    }

    private Range serviceRequest(long sensorId) {
        Range range = null;
        ResponseEntity<Range> responseEntity;
        try {
            responseEntity = restTemplate.exchange(getUrl(sensorId), HttpMethod.GET, null, Range.class);
            if (responseEntity.getStatusCode().is2xxSuccessful()) {
                range = responseEntity.getBody();
                log.debug("Range received from remote service: {}", range);
            } else {
                log.warn("Failed to get range from remote service, status code: {}", responseEntity.getStatusCode());
            }
        } catch (Exception e) {
            log.error("Error retrieving range from remote service: {}", e.getMessage());
            range = RANGE_DEFAULT;
        }
        return range;
    }

    private String getUrl(long sensorId) {
        String url = String.format("http://%s:%d%s%d",
                serviceConfiguration.getHost(),
                serviceConfiguration.getPort(),
                serviceConfiguration.getPath(),
                sensorId);
        log.debug("Generated URL: {}", url);
        return url;
    }
}
