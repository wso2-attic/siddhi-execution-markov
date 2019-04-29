/*
 * Copyright (c)  2016, WSO2 Inc. (http://www.wso2.org) All Rights Reserved.
 *
 * WSO2 Inc. licenses this file to you under the Apache License,
 * Version 2.0 (the "License"); you may not use this file except
 * in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.wso2.extension.siddhi.execution.markov;

import io.siddhi.core.SiddhiAppRuntime;
import io.siddhi.core.SiddhiManager;
import io.siddhi.core.event.Event;
import io.siddhi.core.query.output.callback.QueryCallback;
import io.siddhi.core.stream.input.InputHandler;
import io.siddhi.core.util.EventPrinter;
import org.apache.log4j.Logger;
import org.testng.AssertJUnit;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import java.util.concurrent.CountDownLatch;

import static java.util.concurrent.TimeUnit.MILLISECONDS;

/**
 * Following scenarios will be tested here.
 * Populating matrix from file.
 * MarkovChain continues training.
 * MarkovChain discontinues training.
 */
public class MarkovChainTestCase {
    private static final Logger logger = Logger.getLogger(MarkovChainTestCase.class);
    protected static SiddhiManager siddhiManager;
    private CountDownLatch countDownLatch;
    private volatile int count;
    private volatile boolean eventArrived;

    @BeforeMethod
    public void init() {
        count = 0;
        eventArrived = false;
    }

    @Test
    public void testMarkovChainPopulatingMatrixFromFile() throws Exception {
        logger.info("MarkovChain populating matrix from file test case.");

        final int expectedNoOfEvents = 11;
        countDownLatch = new CountDownLatch(expectedNoOfEvents);
        siddhiManager = new SiddhiManager();
        String inputStream = "define stream InputStream (id string, state string);";

        ClassLoader classLoader = getClass().getClassLoader();
        String markovMatrixStorageLocation = classLoader.getResource("markovMatrix.csv").getPath();

        String executionPlan = ("@info(name = 'query1') "
                + "from InputStream#markov:markovChain(id, state, 60 min, 0.2, \'" + markovMatrixStorageLocation
                + "\', false) " + "select id, lastState, state, transitionProbability, notify "
                + "insert into OutputStream;");
        SiddhiAppRuntime executionPlanRuntime = siddhiManager
                .createSiddhiAppRuntime(inputStream + executionPlan);

        executionPlanRuntime.addCallback("query1", new QueryCallback() {
            @Override
            public void receive(long timeStamp, Event[] inEvents, Event[] removeEvents) {
                EventPrinter.print(timeStamp, inEvents, removeEvents);
                eventArrived = true;
                for (Event event : inEvents) {
                    countDownLatch.countDown();
                    count++;
                    switch (count) {
                        case 1:
                            AssertJUnit.assertEquals(0.0, event.getData(3));
                            AssertJUnit.assertEquals(false, event.getData(4));
                            break;
                        case 2:
                            AssertJUnit.assertEquals(0.0, event.getData(3));
                            AssertJUnit.assertEquals(false, event.getData(4));
                            break;
                        case 3:
                            AssertJUnit.assertEquals(0.3, event.getData(3));
                            AssertJUnit.assertEquals(false, event.getData(4));
                            break;
                        case 4:
                            AssertJUnit.assertEquals(0.0, event.getData(3));
                            AssertJUnit.assertEquals(false, event.getData(4));
                            break;
                        case 5:
                            AssertJUnit.assertEquals(0.6, event.getData(3));
                            AssertJUnit.assertEquals(false, event.getData(4));
                            break;
                        case 6:
                            AssertJUnit.assertEquals(0.6000000000000001, event.getData(3));
                            AssertJUnit.assertEquals(false, event.getData(4));
                            break;
                        case 7:
                            AssertJUnit.assertEquals(0.6, event.getData(3));
                            AssertJUnit.assertEquals(false, event.getData(4));
                            break;
                        case 8:
                            AssertJUnit.assertEquals(0.3, event.getData(3));
                            AssertJUnit.assertEquals(false, event.getData(4));
                            break;
                        case 9:
                            AssertJUnit.assertEquals(0.3, event.getData(3));
                            AssertJUnit.assertEquals(false, event.getData(4));
                            break;
                        case 10:
                            AssertJUnit.assertEquals(0.0, event.getData(3));
                            AssertJUnit.assertEquals(false, event.getData(4));
                            break;
                        case 11:
                            AssertJUnit.assertEquals(0.3, event.getData(3));
                            AssertJUnit.assertEquals(false, event.getData(4));
                            break;
                        default:
                            AssertJUnit.fail();
                    }
                }
            }
        });

        InputHandler inputHandler = executionPlanRuntime.getInputHandler("InputStream");
        executionPlanRuntime.start();

        inputHandler.send(new Object[]{"1", "testState01"});
        inputHandler.send(new Object[]{"2", "testState02"});
        inputHandler.send(new Object[]{"1", "testState03"});
        inputHandler.send(new Object[]{"3", "testState01"});
        inputHandler.send(new Object[]{"3", "testState02"});
        inputHandler.send(new Object[]{"1", "testState01"});
        inputHandler.send(new Object[]{"1", "testState02"});
        inputHandler.send(new Object[]{"2", "testState01"});
        inputHandler.send(new Object[]{"2", "testState03"});
        inputHandler.send(new Object[]{"4", "testState01"});
        inputHandler.send(new Object[]{"4", "testState03"});

        countDownLatch.await(1000, MILLISECONDS);
        AssertJUnit.assertEquals("Number of success events", 11, count);
        AssertJUnit.assertEquals("Event arrived", true, eventArrived);
        executionPlanRuntime.shutdown();
    }

    @Test
    public void testMarkovChainContinuesTraining() throws Exception {
        logger.info("MarkovChain continues training test case.");

        final int expectedNoOfEvents = 6;
        countDownLatch = new CountDownLatch(expectedNoOfEvents);
        siddhiManager = new SiddhiManager();
        String inputStream = "define stream InputStream (id string, state string);";

        String executionPlan = ("@info(name = 'query1') "
                + "from InputStream#markov:markovChain(id, state, 60 min, 0.2, 5) "
                + "select id, lastState, state, transitionProbability, notify "
                + "insert into OutputStream;");
        SiddhiAppRuntime executionPlanRuntime = siddhiManager
                .createSiddhiAppRuntime(inputStream + executionPlan);

        executionPlanRuntime.addCallback("query1", new QueryCallback() {
            @Override
            public void receive(long timeStamp, Event[] inEvents, Event[] removeEvents) {
                EventPrinter.print(timeStamp, inEvents, removeEvents);
                eventArrived = true;
                for (Event event : inEvents) {
                    countDownLatch.countDown();
                    count++;
                    switch (count) {
                        case 1:
                            AssertJUnit.assertEquals(0.0, event.getData(3));
                            AssertJUnit.assertEquals(true, event.getData(4));
                            break;
                        case 2:
                            AssertJUnit.assertEquals(0.5, event.getData(3));
                            AssertJUnit.assertEquals(false, event.getData(4));
                            break;
                        case 3:
                            AssertJUnit.assertEquals(0.0, event.getData(3));
                            AssertJUnit.assertEquals(true, event.getData(4));
                            break;
                        case 4:
                            AssertJUnit.assertEquals(0.3333333333333333, event.getData(3));
                            AssertJUnit.assertEquals(false, event.getData(4));
                            break;
                        case 5:
                            AssertJUnit.assertEquals(0.0, event.getData(3));
                            AssertJUnit.assertEquals(false, event.getData(4));
                            break;
                        case 6:
                            AssertJUnit.assertEquals(0.5, event.getData(3));
                            AssertJUnit.assertEquals(false, event.getData(4));
                            break;
                        default:
                            AssertJUnit.fail();
                    }
                }
            }
        });

        InputHandler inputHandler = executionPlanRuntime.getInputHandler("InputStream");
        executionPlanRuntime.start();

        inputHandler.send(new Object[]{"1", "testState01"});
        inputHandler.send(new Object[]{"2", "testState02"});
        inputHandler.send(new Object[]{"1", "testState03"});
        inputHandler.send(new Object[]{"3", "testState01"});
        inputHandler.send(new Object[]{"3", "testState02"});
        inputHandler.send(new Object[]{"1", "testState01"});
        inputHandler.send(new Object[]{"1", "testState02"});
        inputHandler.send(new Object[]{"2", "testState01"});
        inputHandler.send(new Object[]{"2", "testState03"});
        inputHandler.send(new Object[]{"4", "testState01"});
        inputHandler.send(new Object[]{"4", "testState03"});

        countDownLatch.await(1000, MILLISECONDS);
        AssertJUnit.assertEquals("Number of success events", 6, count);
        AssertJUnit.assertEquals("Event arrived", true, eventArrived);
        executionPlanRuntime.shutdown();
    }

    @Test
    public void testMarkovChainDiscontinuesTraining() throws Exception {
        logger.info("MarkovChain discontinues training test case");

        final int expectedNoOfEvents = 6;
        countDownLatch = new CountDownLatch(expectedNoOfEvents);
        siddhiManager = new SiddhiManager();
        String inputStream = "define stream InputStream (id string, state string, train bool);";

        String executionPlan = ("@info(name = 'query1') "
                + "from InputStream#markov:markovChain(id, state, 60 min, 0.2, 5, train) "
                + "select id, lastState, state, transitionProbability, notify "
                + "insert into OutputStream;");
        SiddhiAppRuntime executionPlanRuntime = siddhiManager
                .createSiddhiAppRuntime(inputStream + executionPlan);

        executionPlanRuntime.addCallback("query1", new QueryCallback() {
            @Override
            public void receive(long timeStamp, Event[] inEvents, Event[] removeEvents) {
                EventPrinter.print(timeStamp, inEvents, removeEvents);
                eventArrived = true;
                for (Event event : inEvents) {
                    countDownLatch.countDown();
                    count++;
                    switch (count) {
                        case 1:
                            AssertJUnit.assertEquals(0.0, event.getData(3));
                            AssertJUnit.assertEquals(true, event.getData(4));
                            break;
                        case 2:
                            AssertJUnit.assertEquals(0.5, event.getData(3));
                            AssertJUnit.assertEquals(false, event.getData(4));
                            break;
                        case 3:
                            AssertJUnit.assertEquals(0.0, event.getData(3));
                            AssertJUnit.assertEquals(true, event.getData(4));
                            break;
                        case 4:
                            AssertJUnit.assertEquals(0.3333333333333333, event.getData(3));
                            AssertJUnit.assertEquals(false, event.getData(4));
                            break;
                        case 5:
                            AssertJUnit.assertEquals(0.0, event.getData(3));
                            AssertJUnit.assertEquals(false, event.getData(4));
                            break;
                        case 6:
                            AssertJUnit.assertEquals(0.3333333333333333, event.getData(3));
                            AssertJUnit.assertEquals(false, event.getData(4));
                            break;
                        default:
                            AssertJUnit.fail();
                    }
                }
            }
        });

        InputHandler inputHandler = executionPlanRuntime.getInputHandler("InputStream");
        executionPlanRuntime.start();

        inputHandler.send(new Object[]{"1", "testState01", true});
        inputHandler.send(new Object[]{"2", "testState02", true});
        inputHandler.send(new Object[]{"1", "testState03", true});
        inputHandler.send(new Object[]{"3", "testState01", true});
        inputHandler.send(new Object[]{"3", "testState02", true});
        inputHandler.send(new Object[]{"1", "testState01", true});
        inputHandler.send(new Object[]{"1", "testState02", true});
        inputHandler.send(new Object[]{"2", "testState01", true});
        inputHandler.send(new Object[]{"2", "testState03", false});
        inputHandler.send(new Object[]{"4", "testState01", false});
        inputHandler.send(new Object[]{"4", "testState03", false});

        countDownLatch.await(1000, MILLISECONDS);
        AssertJUnit.assertEquals("Number of success events", 6, count);
        AssertJUnit.assertEquals("Event arrived", true, eventArrived);
        executionPlanRuntime.shutdown();
    }

}
