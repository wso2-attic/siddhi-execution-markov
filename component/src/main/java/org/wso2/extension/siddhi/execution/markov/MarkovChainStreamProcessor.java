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

import io.siddhi.annotation.Example;
import io.siddhi.annotation.Extension;
import io.siddhi.annotation.Parameter;
import io.siddhi.annotation.ReturnAttribute;
import io.siddhi.annotation.util.DataType;
import io.siddhi.core.config.SiddhiQueryContext;
import io.siddhi.core.event.ComplexEvent;
import io.siddhi.core.event.ComplexEventChunk;
import io.siddhi.core.event.stream.MetaStreamEvent;
import io.siddhi.core.event.stream.StreamEvent;
import io.siddhi.core.event.stream.StreamEventCloner;
import io.siddhi.core.event.stream.holder.StreamEventClonerHolder;
import io.siddhi.core.event.stream.populater.ComplexEventPopulater;
import io.siddhi.core.executor.ConstantExpressionExecutor;
import io.siddhi.core.executor.ExpressionExecutor;
import io.siddhi.core.query.processor.ProcessingMode;
import io.siddhi.core.query.processor.Processor;
import io.siddhi.core.query.processor.SchedulingProcessor;
import io.siddhi.core.query.processor.stream.StreamProcessor;
import io.siddhi.core.util.Scheduler;
import io.siddhi.core.util.config.ConfigReader;
import io.siddhi.core.util.snapshot.state.State;
import io.siddhi.core.util.snapshot.state.StateFactory;
import io.siddhi.query.api.definition.AbstractDefinition;
import io.siddhi.query.api.definition.Attribute;
import io.siddhi.query.api.exception.SiddhiAppValidationException;

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
                "when carrying out real time analysis. There are two approaches to using this extension.\n" +
                "1. Input an existing Markov matrix as a csv file. It should be an N x N matrix " +
                "and the first row should include state names." +
                "2. Use a reasonable amount of incoming data to train a Markov matrix and then use it to" +
                "   create further notifications.",
        parameters = {
                @Parameter(
                        name = "id",
                        description = "The ID of the particular user or object that needs to be analyzed.",
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
                        description = "The location of the CSV file that contains, the existing Markov " +
                                "matrix to be used which is of 'String' type or the notifications hold limit " +
                                "which is of either 'int' or 'long' type.",
                        type = {DataType.INT, DataType.LONG, DataType.STRING}
                ),
                @Parameter(
                        name = "train",
                        description = "If this is set to 'true', event values are used to train the Markov matrix. " +
                                "If this is set to 'false', the Markov matrix values remain the same.",
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
                @Example(syntax = "define stream InputStream (id string, state string);\n" +
                                "from InputStream#markov:markovChain(id, state, 60 min, 0.2, " +
                                "\"markovMatrixStorageLocation\", false)\n" +
                                "select id, lastState, state, transitionProbability, notify\n" +
                                "insert into OutputStream;",
                        description = "The function returns a notification to the 'OutputStream' along with the id, " +
                                "its previous state, current state and the transition probability," +
                                " when a transition probability between the previous state and the " +
                                "current state is less than or equal " +
                                "to 0.2. The 'transitionProbability' is based on the Markov matrix that is already " +
                                "provided since 'false' indicates that the event values are not used to train" +
                                "the matrix.\n"

                )
        }
)
public class MarkovChainStreamProcessor
        extends StreamProcessor<MarkovChainStreamProcessor.ExtensionState> implements SchedulingProcessor {

    private static final String PROBABILITIES_CALCULATOR = "PROBABILITIES_CALCULATOR";
    private static final String TRAINING_OPTION = "TRAINING_OPTION";
    private static final String TRAINING_OPTION_EXPRESSION_EXECUTOR = "TRAINING_OPTION_EXPRESSION_EXECUTOR";
    private static final String LAST_SCHEDULED_TIME = "LAST_SCHEDULED_TIME";
    private Scheduler scheduler = null;
    private long durationToKeep;
    private long notificationsHoldLimit;
    private String markovMatrixStorageLocation;
    private Boolean trainingOption;
    private ExpressionExecutor trainingOptionExpressionExecutor;
    private MarkovChainTransitionProbabilitiesCalculator markovChainTransitionProbabilitiesCalculator;
    private long lastScheduledTime;

    private List<Attribute> attributes;
    @Override
    public void start() {
    }

    @Override
    public void stop() {
    }

    @Override
    public Scheduler getScheduler() {
        synchronized (this) {
            return this.scheduler;
        }
    }

    @Override
    public void setScheduler(Scheduler scheduler) {
        synchronized (this) {
            this.scheduler = scheduler;
        }
    }

    @Override
    protected void process(ComplexEventChunk<StreamEvent> complexEventChunk,
                           Processor processor,
                           StreamEventCloner streamEventCloner,
                           ComplexEventPopulater complexEventPopulater,
                           ExtensionState extensionState) {
        synchronized (this) {
            while (complexEventChunk.hasNext()) {
                StreamEvent streamEvent = complexEventChunk.next();
                if (streamEvent.getType() == ComplexEvent.Type.TIMER) {
                    markovChainTransitionProbabilitiesCalculator.removeExpiredEvents(
                                    siddhiQueryContext.getSiddhiAppContext().getTimestampGenerator().currentTime());
                    continue;
                } else if (streamEvent.getType() != ComplexEvent.Type.CURRENT) {
                    continue;
                }
                long nextScheduledTime = siddhiQueryContext
                        .getSiddhiAppContext().getTimestampGenerator().currentTime() + durationToKeep;
                if (scheduler != null && nextScheduledTime > lastScheduledTime) {
                    scheduler.notifyAt(nextScheduledTime);
                    lastScheduledTime = nextScheduledTime;
                }
                if (trainingOptionExpressionExecutor != null) {
                    trainingOption = (Boolean) attributeExpressionExecutors[5].execute(streamEvent);
                }
                String id = (String) attributeExpressionExecutors[0].execute(streamEvent);
                String state = (String) attributeExpressionExecutors[1].execute(streamEvent);
                Object[] outputData = markovChainTransitionProbabilitiesCalculator.processData(id, state,
                        trainingOption);

                if (outputData == null) {
                    complexEventChunk.remove();
                } else {
                    complexEventPopulater.populateComplexEvent(streamEvent, outputData);
                }
            }
        }
        nextProcessor.process(complexEventChunk);
    }

    @Override
    protected StateFactory<ExtensionState> init(MetaStreamEvent metaStreamEvent,
                                                AbstractDefinition abstractDefinition,
                                                ExpressionExecutor[] attributeExpressionExecutors,
                                                ConfigReader configReader,
                                                StreamEventClonerHolder streamEventClonerHolder,
                                                boolean b1,
                                                boolean b2,
                                                SiddhiQueryContext siddhiQueryContext) {
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

        attributes = new ArrayList<Attribute>(3);
        attributes.add(new Attribute("lastState", Attribute.Type.STRING));
        attributes.add(new Attribute("transitionProbability", Attribute.Type.DOUBLE));
        attributes.add(new Attribute("notify", Attribute.Type.BOOL));

        return () -> new ExtensionState();
    }

    @Override
    public List<Attribute> getReturnAttributes() {
        return attributes;
    }

    @Override
    public ProcessingMode getProcessingMode() {
        return ProcessingMode.BATCH;
    }

    private enum TrainingMode {
        PREDEFINED_MATRIX, REAL_TIME
    }

    class ExtensionState extends State {

        @Override
        public boolean canDestroy() {
            return false;
        }

        @Override
        public Map<String, Object> snapshot() {
            synchronized (MarkovChainStreamProcessor.this) {
                Map<String, Object> state = new HashMap<>(4);
                state.put(PROBABILITIES_CALCULATOR, markovChainTransitionProbabilitiesCalculator);
                state.put(TRAINING_OPTION, trainingOption);
                state.put(TRAINING_OPTION_EXPRESSION_EXECUTOR, trainingOptionExpressionExecutor);
                state.put(LAST_SCHEDULED_TIME, lastScheduledTime);

                return state;
            }
        }

        @Override
        public void restore(Map<String, Object> map) {
            synchronized (MarkovChainStreamProcessor.this) {
                markovChainTransitionProbabilitiesCalculator =
                        (MarkovChainTransitionProbabilitiesCalculator) map.get(PROBABILITIES_CALCULATOR);
                trainingOption = (Boolean) map.get(TRAINING_OPTION);
                trainingOptionExpressionExecutor = (ExpressionExecutor) map.get(TRAINING_OPTION_EXPRESSION_EXECUTOR);
                lastScheduledTime = (Long) map.get(LAST_SCHEDULED_TIME);
            }
        }
    }
}
