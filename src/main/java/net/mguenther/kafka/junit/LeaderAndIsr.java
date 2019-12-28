package net.mguenther.kafka.junit;

import lombok.Getter;
import lombok.RequiredArgsConstructor;
import lombok.ToString;

import java.util.Set;

@Getter
@ToString
@RequiredArgsConstructor
public class LeaderAndIsr {

    private final Integer leader;
    private final Set<Integer> isr;

    /**
     * @deprecated This method will be removed in a future release. Please use {@code LeaderAndIsr#getLeader()} instead.
     */
    @Deprecated
    public Integer leader() {
        return leader;
    }
}
