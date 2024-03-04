package telran.probes.dto;

import jakarta.validation.Valid;
import jakarta.validation.constraints.*;
import static telran.probes.messages.ErrorMessages.*;
public record SensorRange(@NotNull(message=MISING_SENSOR_ID) long id, @Valid @NotNull(message=MISSING_RANGE) Range range) {

}
