package org.k1den.util;

import org.junit.jupiter.api.Test;
import java.util.HashMap;
import java.util.Map;
import static org.junit.jupiter.api.Assertions.*;

class JsonUtilsTest {

    @Test
    void testSerializationAndDeserialization() {
        Map<String, Object> testMetric = new HashMap<>();
        testMetric.put("deviceId", "server-01");
        testMetric.put("cpuLoad", 45.5);

        String jsonString = JsonUtils.toJson(testMetric);
        assertNotNull(jsonString, "JSON строка не должна быть пустой");
        assertTrue(jsonString.contains("\"server-01\""), "JSON должен содержать ID устройства");

        Map parsedMetric = JsonUtils.fromJson(jsonString, Map.class);
        assertEquals("server-01", parsedMetric.get("deviceId"), "ID устройства должен совпадать после парсинга");
        assertEquals(45.5, parsedMetric.get("cpuLoad"), "Значение метрики должно совпадать");
    }
}