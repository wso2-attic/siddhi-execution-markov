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

import org.wso2.siddhi.annotation.Example;
import org.wso2.siddhi.annotation.Extension;
import org.wso2.siddhi.annotation.Parameter;
import org.wso2.siddhi.annotation.ReturnAttribute;
import org.wso2.siddhi.annotation.util.DataType;
import org.wso2.siddhi.core.config.SiddhiAppContext;
import org.wso2.siddhi.core.event.ComplexEvent;
import org.wso2.siddhi.core.event.ComplexEventChunk;
import org.wso2.siddhi.core.event.stream.StreamEvent;
import org.wso2.siddhi.core.event.stream.StreamEventCloner;
import org.wso2.siddhi.core.event.stream.populater.ComplexEventPopulater;
import org.wso2.siddhi.core.executor.ConstantExpressionExecutor;
import org.wso2.siddhi.core.executor.ExpressionExecutor;
import org.wso2.siddhi.core.query.processor.Processor;
import org.wso2.siddhi.core.query.processor.SchedulingProcessor;
import org.wso2.siddhi.core.query.processor.stream.StreamProcessor;
import org.wso2.siddhi.core.util.Scheduler;
import org.wso2.siddhi.core.util.config.ConfigReader;
import org.wso2.siddhi.query.api.definition.AbstractDefinition;
import org.wso2.siddhi.query.api.definition.Attribute;
import org.wso2.siddhi.query.api.exception.SiddhiAppValidationException;

import java.io.File;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * StreamProcessor which implements the following function.
 * <code>markovChain( id, state, durationToKeep, alertThreshold,
 * notificationsHoldLimit/markovMatrixStorageLocation, train )</code>
 * Returns last state, transition probability and notification.
 * Accept Type(s): STRING, STRING, INT/LONG/TIME, DOUBLE, (INT/LONG, STRING), BOOLEAN
 * Return Type: STRING, DOUBLE, BOOLEAN
 */
@Extension(
        name = "markovChain",
        namespace = "markov",
        description = "The Markov Models extension allows abnormal patterns relating to user activity to be detected " +
                "when carrying out real time analysis. There are two approaches for using this extension." +
                "1. You can input an existing Markov matrix as a csv file. It should be a N x N matrix, " +
                "   and the first row should include state names." +
                "2. You can use a reasonable amount of incoming data to train a Markov matrix and then using it to" +
                "   create notifications.",
        parameters = {
                @Parameter(
                        name = "id",
                        description = "The ID of the particular user or object being analyzed.",
                        type = {DataType.STRING}
                ),
                @Parameter(
                        name = "state",
                        description = "The current state of the ID.",
                        type = {DataType.STRING}
                ),
                @Parameter(
                        name = "duration.to.keep",
                        description = "The maximum time duration to be considered for a continuous state change " +
                                "of a particular ID.",
                        type = {DataType.INT, DataType.LONG}
                ),
                @Parameter(
                        name = "alert.threshold",
                        description = "The alert threshold probability.",
                        type = {DataType.DOUBLE}
                ),
                @Parameter(
                        name = "matrix.location.or.notifications.limit",
                        description = "The location of the CSV file that contains the existing Markov " +
                                "matrix to be used (string) or the notifications hold limit (int/long)",
                        type = {DataType.INT, DataType.LONG, DataType.STRING}
                ),
                @Parameter(
                        name = "train",
                        description = "If this is set to true, event values are used to train the Markov matrix. " +
                                "If this is set to false, the Markov matrix values remain the same.",
                        type = {DataType.BOOL},
                        defaultValue = "true",
                        optional = true
                )
        },
        returnAttributes = {
                @ReturnAttribute(
                        name = "lastState",
                        type = {DataType.STRING},
                        description = "The previous state of the particular ID."
                ),
                @ReturnAttribute(
                        name = "transitionProbability",
                        type = {DataType.DOUBLE},
                        description = "The transition probability between the previous state and the current state " +
                                "for a particular ID."
                ),
                @ReturnAttribute(
                        name = "notify",
                        type = {DataType.BOOL},
                        description = "This signifies a notification that indicates that the transition probability " +
                                "is less than or equal to the alert threshold probability."
                )
        },
        examples = {
                @Example(syntax = "markov:markovChain(<String> id, <String> state, <int|long|time> durationToKeep, " +
                        "<double> alertThreshold, <String> markovMatrixStorageLocation, <boolean> train)",
                        description = "The following returns notifications to indicate whether a transition " +
                                "probability is less than or equal to 0.2 according to the Markov matrix you have" +
                                " provided. " +
                                "\n" +
                                "define stream InputStream (id string, state string);\n" +
                                "from InputStream#markov:markovChain(id, state, 60 min, 0.2, " +
                                "\"markovMatrixStorageLocation\", false)\n" +
                                "select id, lastState, state, transitionProbability, notify\n" +
                                "insert into OutputStream;"
                )
        }
)
public class MarkovChainStreamProcessor extends StreamProcessor implements SchedulingProcessor {

    private static final String PROBABILITIES_CALCULATOR = "PROBABILITIES_CALCULATOR";
    private static final String TRAINING_OPTION = "TRAINING_OPTION";
    private static final String TRAINING_OPTION_EXPRESSION_EXECUTOR = "TRAINING_OPTION_EXPRESSION_EXECUTOR";
    private static final String LAST_SCHEDULED_TIME = "LAST_SCHEDULED_TIME";
    private Scheduler scheduler;
    private long durationToKeep;
    private long notificationsHoldLimit;
    private String markovMatrixStorageLocation;
    private Boolean trainingOption;
    private ExpressionExecutor trainingOptionExpressionExecutor;
    private MarkovChainTransitionProbabilitiesCalculator markovChainTransitionProbabilitiesCalculator;
    private long lastScheduledTime;

    @Override
    protected List<Attribute> init(AbstractDefinition abstractDefinition, ExpressionExecutor[] expressionExecutors,
                                   ConfigReader configReader, SiddhiAppContext siddhiAppContext) {

        if (!(attributeExpressionExecutors.length == 5 || attributeExpressionExecutors.length == 6)) {
            throw new SiddhiAppValidationException(
                    "Markov chain function has to have exactly 5 or 6 parameters, currently "
                            + attributeExpressionExecutors.length + " parameters provided.");
        }

        trainingOption = true;
        TrainingMode trainingMode = TrainingMode.REAL_TIME;

        if (!(attributeExpressionExecutors[2] instanceof ConstantExpressionExecutor)) {
            throw new SiddhiAppValidationException("Duration has to be a constant.");
        }

        if (!(attributeExpressionExecutors[3] instanceof ConstantExpressionExecutor)) {
            throw new SiddhiAppValidationException("Alert threshold probability value has to be a constant.");
        }

        if (!(attributeExpressionExecutors[4] instanceof ConstantExpressionExecutor)) {
            throw new SiddhiAppValidationException("Training batch size has to be a constant.");
        }

        Object durationObject = attributeExpressionExecutors[2].execute(null);
        if (durationObject instanceof Integer) {
            durationToKeep = (Integer) durationObject;
        } else if (durationObject instanceof Long) {
            durationToKeep = (Long) durationObject;
        } else {
            throw new SiddhiAppValidationException("Duration should be of type int or long. But found "
                    + attributeExpressionExecutors[2].getReturnType());
        }

        Object alertThresholdProbabilityObject = attributeExpressionExecutors[3].execute(null);
        double alertThresholdProbability;
        if (alertThresholdProbabilityObject instanceof Double) {
            alertThresholdProbability = (Double) alertThresholdProbabilityObject;
        } else {
            throw new SiddhiAppValidationException(
                    "Alert threshold probability should be of type double. But found "
                            + attributeExpressionExecutors[3].getReturnType());
        }

        Object object = attributeExpressionExecutors[4].execute(null);
        if (object instanceof String) {
            markovMatrixStorageLocation = (String) object;
            trainingMode = TrainingMode.PREDEFINED_MATRIX;

            File file = new File(markovMatrixStorageLocation);
            if (!file.exists()) {
                throw new SiddhiAppValidationException(
                        markovMatrixStorageLocation + " does not exist. Please provide a valid file path.");
            } else if (!file.isFile()) {
                throw new SiddhiAppValidationException(
                        markovMatrixStorageLocation + " is not a file. Please provide a valid csv file.");
            }
        } else if (object instanceof Integer) {
            notificationsHoldLimit = (Integer) object;
        } else if (object instanceof Long) {
            notificationsHoldLimit = (Long) object;
        } else {
            throw new SiddhiAppValidationException(
                    "5th parameter should be the Training batch size or Markov matrix storage location. "
                            + "They should be of types int/long or String. But found "
                            + attributeExpressionExecutors[4].getReturnType());
        }

        if (attributeExpressionExecutors.length == 6) {
            if (attributeExpressionExecutors[5] instanceof ConstantExpressionExecutor) {
                Object trainingOptionObject = attributeExpressionExecutors[5].execute(null);
                if (trainingOptionObject instanceof Boolean) {
                    trainingOption = (Boolean) trainingOptionObject;
                } else {
                    throw new SiddhiAppValidationException("Training option should be of type boolean. But found "
                            + attributeExpressionExecutors[5].getReturnType());
                }
            } else {
                trainingOptionExpressionExecutor = attributeExpressionExecutors[5];
            }
        }

        if (trainingMode == TrainingMode.PREDEFINED_MATRIX) {
            markovChainTransitionProbabilitiesCalculator = new MarkovChainTransitionProbabilitiesCalculator(
                    durationToKeep, alertThresholdProbability, markovMatrixStorageLocation);
        } else {
            markovChainTransitionProbabilitiesCalculator = new MarkovChainTransitionProbabilitiesCalculator(
                    durationToKeep, alertThresholdProbability, notificationsHoldLimit);
        }

        List<Attribute> attributeList = new ArrayList<Attribute>(3);
        attributeList.add(new Attribute("lastState", Attribute.Type.STRING));
        attributeList.add(new Attribute("transitionProbability", Attribute.Type.DOUBLE));
        attributeList.add(new Attribute("notify", Attribute.Type.BOOL));
        return attributeList;
    }

    @Override
    protected void process(ComplexEventChunk<StreamEvent> streamEventChunk, Processor nextProcessor,
                           StreamEventCloner streamEventCloner, ComplexEventPopulater complexEventPopulater) {

        synchronized (this) {
            while (streamEventChunk.hasNext()) {
                StreamEvent streamEvent = streamEventChunk.next();

                if (streamEvent.getType() == ComplexEvent.Type.TIMER) {
                    markovChainTransitionProbabilitiesCalculator
                            .removeExpiredEvents(siddhiAppContext.getTimestampGenerator().currentTime());
                    continue;
                } else if (streamEvent.getType() != ComplexEvent.Type.CURRENT) {
                    continue;
                }
                lastScheduledTime = siddhiAppContext.getTimestampGenerator().currentTime() + durationToKeep;
                scheduler.notifyAt(lastScheduledTime);

                if (trainingOptionExpressionExecutor != null) {
                    trainingOption = (Boolean) attributeExpressionExecutors[5].execute(streamEvent);
                }
                String id = (String) attributeExpressionExecutors[0].execute(streamEvent);
                String state = (String) attributeExpressionExecutors[1].execute(streamEvent);
                Object[] outputData = markovChainTransitionProbabilitiesCalculator.processData(id, state,
                        trainingOption);

                if (outputData == null) {
                    streamEventChunk.remove();
                } else {
                    complexEventPopulater.populateComplexEvent(streamEvent, outputData);
                }
            }
        }
        nextProcessor.process(streamEventChunk);
    }

    @Override
    public void start() {

    }

    @Override
    public void stop() {

    }

    @Override
    public Map<String, Object> currentState() {
        Map<String, Object> state = new HashMap<>(4);
        state.put(PROBABILITIES_CALCULATOR, markovChainTransitionProbabilitiesCalculator);
        state.put(TRAINING_OPTION, trainingOption);
        state.put(TRAINING_OPTION_EXPRESSION_EXECUTOR, trainingOptionExpressionExecutor);
        state.put(LAST_SCHEDULED_TIME, lastScheduledTime);
        return state;
    }

    @Override
    public void restoreState(Map<String, Object> map) {
        markovChainTransitionProbabilitiesCalculator =
                (MarkovChainTransitionProbabilitiesCalculator) map.get(PROBABILITIES_CALCULATOR);
        trainingOption = (Boolean) map.get(TRAINING_OPTION);
        trainingOptionExpressionExecutor = (ExpressionExecutor) map.get(TRAINING_OPTION_EXPRESSION_EXECUTOR);
        lastScheduledTime = (Long) map.get(LAST_SCHEDULED_TIME);
    }

    @Override
    public Scheduler getScheduler() {
        return this.scheduler;
    }

    @Override
    public void setScheduler(Scheduler scheduler) {
        this.scheduler = scheduler;
    }

    private enum TrainingMode {
        PREDEFINED_MATRIX, REAL_TIME
    }

}
