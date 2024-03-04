package telran.probes.dto;

import jakarta.validation.constraints.NotNull;

public record Range(@NotNull double minValue, @NotNull double maxValue) {

}
