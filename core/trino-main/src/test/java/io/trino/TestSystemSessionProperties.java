/*
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package io.trino;

import io.airlift.units.Duration;
import io.trino.spi.session.PropertyMetadata;
import org.junit.jupiter.api.Test;

import static io.trino.SystemSessionProperties.QUERY_MAX_RUN_TIME;
import static org.assertj.core.api.Assertions.assertThatNoException;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

public class TestSystemSessionProperties
{
    private static final SystemSessionProperties properties = new SystemSessionProperties();

    @Test
    public void testQueryMaxRunTime()
    {
        PropertyMetadata<Duration> property = getProperty(QUERY_MAX_RUN_TIME);
        assertThatNoException().isThrownBy(() -> property.decode("1h"));
        assertThatThrownBy(() -> property.decode("1000d"))
                .hasMessage("query_max_run_time must not exceed default 100.00d: 1000.00d");
    }

    @SuppressWarnings("unchecked")
    private <T> PropertyMetadata<T> getProperty(String propertyName)
    {
        return (PropertyMetadata<T>) properties.getSessionProperties().stream()
                .filter(p -> p.getName().equals(propertyName))
                .findFirst()
                .orElseThrow();
    }
}
