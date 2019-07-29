package com.wepay.kafka.connect.bigquery.preprocess;

import com.jayway.jsonpath.InvalidPathException;
import com.wepay.kafka.connect.bigquery.SinkConnectorPropertiesFactory;
import com.wepay.kafka.connect.bigquery.config.BigQuerySinkConfig;
import org.hamcrest.CoreMatchers;
import org.hamcrest.Matcher;
import org.junit.After;
import org.junit.BeforeClass;
import org.junit.Test;
import uk.org.lidalia.slf4jtest.LoggingEvent;
import uk.org.lidalia.slf4jtest.TestLogger;
import uk.org.lidalia.slf4jtest.TestLoggerFactory;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.junit.Assert.*;
import static org.junit.jupiter.api.Assertions.assertThrows;


public class SinkRecordFilterTest {
    private static final String INCLUDE_CONDITION_CONFIG = "includeCondition";
    private static final String EXCLUDE_CONDITION_CONFIG = "excludeCondition";
    private static Map testMap;
    private SinkRecordFilter sinkRecordFilter;
    private static final TestLogger testLogger = TestLoggerFactory.getTestLogger(SinkRecordFilter.class);


    public static SinkRecordFilter initializeSinkRecordFilter(String prop, String condition) {
        Map<String, String> configProperties = new SinkConnectorPropertiesFactory().getProperties();
        if (prop != null) {
            configProperties.put(prop, condition);
        }
        return new SinkRecordFilter(new BigQuerySinkConfig(configProperties));
    }



    @BeforeClass
    public static void initializeTestMap() {
        // The map looks like:
        // {
        //     Inventories = [
        //         {FINANCIAL_BLOCK=Balance Sheet, COUNTRY_ID=8},
        //         {FINANCIAL_BLOCK=Income Statement, COUNTRY_ID=8}
        //        ],
        //     High Performance Mixed Signal ( HPMS ) =
        //         [{FINANCIAL_BLOCK=Income Statement, COUNTRY_ID=8}]
        // }

        testMap = new HashMap<>();

        List<Map<String ,Object>> inventoriesList = new ArrayList<>();
        Map<String ,Object> map1 = new HashMap<>();
        map1.put("FINANCIAL_BLOCK", "Balance Sheet");
        map1.put("COUNTRY_ID", 8);

        inventoriesList.add(map1);


        Map<String ,Object> map2 = new HashMap<>();
        map2.put("FINANCIAL_BLOCK", "Income Statement");
        map2.put("COUNTRY_ID", 8);

        inventoriesList.add(map2);

        testMap.put("Inventories", inventoriesList);

        List<Map<String ,Object>> highPerformanceList = new ArrayList<>();
        highPerformanceList.add(map2);

        testMap.put("High Performance Mixed Signal ( HPMS )", highPerformanceList);

    }


    @Test
    public void testFilterConstructor() {
        // test when includeCondition and excludeCondition are not configured
        sinkRecordFilter = initializeSinkRecordFilter(null, null);
        assertNull(sinkRecordFilter.getConfigString());
        assertNull(sinkRecordFilter.getJsonPathCondition());
        assertNull(sinkRecordFilter.getConfigType());

        // test when configured path is invalid
        assertThrows(InvalidPathException.class, ()->
            sinkRecordFilter = initializeSinkRecordFilter(INCLUDE_CONDITION_CONFIG, "$(.NoRightParenthesis")
        );

        // test when configured path is valid
        sinkRecordFilter = initializeSinkRecordFilter(EXCLUDE_CONDITION_CONFIG, "$.Inventories");
        assertEquals(sinkRecordFilter.getConfigType(), EXCLUDE_CONDITION_CONFIG);
        assertEquals(sinkRecordFilter.getConfigString(), "$.Inventories");
    }

    @Test
    public void testExistJsonPath(){

        // test when there's at least one node that satisfies condition
        sinkRecordFilter = initializeSinkRecordFilter(INCLUDE_CONDITION_CONFIG,
                "$.Inventories[?(!!@..FINANCIAL_BLOCK)]");

        assertEquals(sinkRecordFilter.getConfigType(), INCLUDE_CONDITION_CONFIG);
        assertEquals(sinkRecordFilter.getFilterFields(testMap).size(), 2);
        assertEquals(sinkRecordFilter.satisfyCondition(testMap), true);

        sinkRecordFilter = initializeSinkRecordFilter(INCLUDE_CONDITION_CONFIG,
                "$[?((@.Inventories[0].COUNTRY_ID == 8 && @.Inventories[1].COUNTRY_ID < 8) || " +
                        "@['High Performance Mixed Signal ( HPMS )'][0].COUNTRY_ID == 8)]");

        assertEquals(sinkRecordFilter.getConfigType(), INCLUDE_CONDITION_CONFIG);
        assertEquals(sinkRecordFilter.getFilterFields(testMap).size(), 1);
        assertEquals(sinkRecordFilter.satisfyCondition(testMap), true);

        // test when node does not satisfies condition
        sinkRecordFilter = initializeSinkRecordFilter(INCLUDE_CONDITION_CONFIG,
                "$.Inventories[?(@..FINANCIAL_BLOCK == 'Balance Sheet')]");

        assertEquals(sinkRecordFilter.getConfigType(), INCLUDE_CONDITION_CONFIG);
        assertEquals(sinkRecordFilter.getFilterFields(testMap).size(), 0);
        assertEquals(sinkRecordFilter.satisfyCondition(testMap), false);

        sinkRecordFilter = initializeSinkRecordFilter(INCLUDE_CONDITION_CONFIG,
                "$['High Performance Mixed Signal ( HPMS )'][0][?(@.FINANCIAL_BLOCK == 'Balance Sheet')]");

        assertEquals(sinkRecordFilter.getConfigType(), INCLUDE_CONDITION_CONFIG);
        assertEquals(sinkRecordFilter.getFilterFields(testMap).size(), 0);
        assertEquals(sinkRecordFilter.satisfyCondition(testMap), false);

        // test when only path is given, no condition(expected to return nodes corresponding to JsonPath)
        sinkRecordFilter = initializeSinkRecordFilter(INCLUDE_CONDITION_CONFIG,
                "$.Inventories[?(@.FINANCIAL_BLOCK)]");

        assertEquals(sinkRecordFilter.getConfigType(), INCLUDE_CONDITION_CONFIG);
        assertEquals(sinkRecordFilter.getFilterFields(testMap).size(), 2);
        assertEquals(sinkRecordFilter.satisfyCondition(testMap), true);

    }

    @Test
    public void testNotExistJsonPath(){
        SinkRecordFilter sinkRecordFilter = initializeSinkRecordFilter(INCLUDE_CONDITION_CONFIG,
                "$.Inventories[0].nonexitentField");


        List filtered = sinkRecordFilter.getFilterFields(testMap);
        assertEquals(filtered.size(), 0);

        // test log
        assertTrue(assertLogged(testLogger, CoreMatchers.containsString(
                "config can not be found in json object")));
    }

    @Test
    public void testNull() {
        sinkRecordFilter = initializeSinkRecordFilter(INCLUDE_CONDITION_CONFIG, null);
        assertEquals(sinkRecordFilter.getFilterFields(testMap).size(), 0);
        assertEquals(sinkRecordFilter.getFilterFields(null).size(), 0);
    }

    @After
    public void clearLoggers() {
        TestLoggerFactory.clear();
    }

    private boolean assertLogged(TestLogger testLogger, Matcher matcher) {
        for(LoggingEvent event : testLogger.getAllLoggingEvents()) {
            if(matcher.matches(event.getMessage())) {
                return true;
            }
        }
        return false;
    }
}
