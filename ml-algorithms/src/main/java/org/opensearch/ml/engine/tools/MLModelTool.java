/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.ml.engine.tools;

import java.util.List;
import java.util.Map;

import org.opensearch.action.ActionRequest;
import org.opensearch.client.Client;
import org.opensearch.core.action.ActionListener;
import org.opensearch.ml.common.FunctionName;
import org.opensearch.ml.common.dataset.remote.RemoteInferenceInputDataSet;
import org.opensearch.ml.common.input.MLInput;
import org.opensearch.ml.common.output.model.ModelTensorOutput;
import org.opensearch.ml.common.output.model.ModelTensors;
import org.opensearch.ml.common.spi.tools.Parser;
import org.opensearch.ml.common.spi.tools.Tool;
import org.opensearch.ml.common.spi.tools.ToolAnnotation;
import org.opensearch.ml.common.transport.MLTaskResponse;
import org.opensearch.ml.common.transport.prediction.MLPredictionTaskAction;
import org.opensearch.ml.common.transport.prediction.MLPredictionTaskRequest;

import lombok.Getter;
import lombok.Setter;
import lombok.extern.log4j.Log4j2;

/**
 * This tool supports running any ml-commons model.
 */
@Log4j2
@ToolAnnotation(MLModelTool.TYPE)
public class MLModelTool implements Tool {
    public static final String TYPE = "MLModelTool";

    @Setter
    @Getter
    private String name = TYPE;
    private static String DEFAULT_DESCRIPTION = "Use this tool to run any model.";
    @Getter
    @Setter
    private String description = DEFAULT_DESCRIPTION;
    private Client client;
    private String modelId;
    @Setter
    private Parser inputParser;
    @Setter
    private Parser outputParser;

    public MLModelTool(Client client, String modelId) {
        this.client = client;
        this.modelId = modelId;

        outputParser = o -> {
            List<ModelTensors> mlModelOutputs = (List<ModelTensors>) o;
            return mlModelOutputs.get(0).getMlModelTensors().get(0).getDataAsMap().get("response");
        };
    }

    @Override
    public <T> void run(Map<String, String> parameters, ActionListener<T> listener) {
        RemoteInferenceInputDataSet inputDataSet = RemoteInferenceInputDataSet.builder().parameters(parameters).build();
        ActionRequest request = new MLPredictionTaskRequest(
            modelId,
            MLInput.builder().algorithm(FunctionName.REMOTE).inputDataset(inputDataSet).build()
        );
        client.execute(MLPredictionTaskAction.INSTANCE, request, ActionListener.<MLTaskResponse>wrap(r -> {
            ModelTensorOutput modelTensorOutput = (ModelTensorOutput) r.getOutput();
            modelTensorOutput.getMlModelOutputs();
            listener.onResponse((T) outputParser.parse(modelTensorOutput.getMlModelOutputs()));
        }, e -> {
            log.error("Failed to run model " + modelId, e);
            listener.onFailure(e);
        }));
    }

    @Override
    public String getType() {
        return TYPE;
    }

    @Override
    public String getVersion() {
        return null;
    }

    @Override
    public String getName() {
        return this.name;
    }

    @Override
    public void setName(String s) {
        this.name = s;
    }

    @Override
    public boolean validate(Map<String, String> parameters) {
        if (parameters == null || parameters.size() == 0) {
            return false;
        }
        return true;
    }

    public static class Factory implements Tool.Factory<MLModelTool> {
        private Client client;

        private static Factory INSTANCE;

        public static Factory getInstance() {
            if (INSTANCE != null) {
                return INSTANCE;
            }
            synchronized (MLModelTool.class) {
                if (INSTANCE != null) {
                    return INSTANCE;
                }
                INSTANCE = new Factory();
                return INSTANCE;
            }
        }

        public void init(Client client) {
            this.client = client;
        }

        @Override
        public MLModelTool create(Map<String, Object> map) {
            return new MLModelTool(client, (String) map.get("model_id"));
        }

        @Override
        public String getDefaultDescription() {
            return DEFAULT_DESCRIPTION;
        }
    }
}
