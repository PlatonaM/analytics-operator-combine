/*
 * Copyright 2021 InfAI (CC SES)
 *
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


import com.google.gson.reflect.TypeToken;
import org.infai.ses.platonam.util.Json;
import org.infai.ses.senergy.exceptions.NoValueException;
import org.infai.ses.senergy.operators.BaseOperator;
import org.infai.ses.senergy.operators.Message;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import java.util.logging.Logger;

import static org.infai.ses.platonam.util.Logger.getLogger;


public class Combine extends BaseOperator {
    private static final Logger logger = getLogger(Combine.class.getName());
    private final Set<String> uniqueOutputFields;
    private final Map<String, String> inputMap;

    public Combine(String uniqueOutputFields, Map<String, String> inputMap) {
        this.uniqueOutputFields = uniqueOutputFields != null ? Json.fromString(uniqueOutputFields, new TypeToken<>() {
        }) : Collections.emptySet();
        this.inputMap = inputMap;
    }

    private void outputMessage(Message message, Map<String, Object> combinedOutput, Map<String, Object> uniqueOutputs) {
        message.output("combined_data", Json.toString(new TypeToken<Map<String, Object>>() {
        }.getType(), combinedOutput));
        for (Map.Entry<String, Object> entry : uniqueOutputs.entrySet()) {
            message.output(entry.getKey(), entry.getValue());
        }
    }

    private void allocateInput(String inputKey, Object inputValue, Map<String, Object> combinedOutput, Map<String, Object> uniqueOutputs) {
        if (uniqueOutputFields.contains(inputKey)) {
            uniqueOutputs.put(inputKey, inputValue);
        } else {
            combinedOutput.put(inputKey, inputValue);
        }
    }

    @Override
    public void run(Message message) {
        try {
            Map<String, Object> combinedOutput = new HashMap<>();
            Map<String, Object> uniqueOutputs = new HashMap<>();
            for (Map.Entry<String, String> entry : inputMap.entrySet()) {
                try {
                    Object valueObj = message.getInput(entry.getKey()).getValue(Object.class);
                    allocateInput(entry.getValue(), valueObj, combinedOutput, uniqueOutputs);
                } catch (NoValueException e) {
                    allocateInput(entry.getValue(), null, combinedOutput, uniqueOutputs);
                }
            }
            outputMessage(message, combinedOutput, uniqueOutputs);
        } catch (Throwable t) {
            logger.severe("error handling message:");
            t.printStackTrace();
        }
    }

    @Override
    public Message configMessage(Message message) {
        for (String key : inputMap.keySet()) {
            message.addInput(key);
        }
        return message;
    }
}
