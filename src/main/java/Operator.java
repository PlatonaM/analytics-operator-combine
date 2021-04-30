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


import org.infai.ses.platonam.util.Logger;
import org.infai.ses.senergy.operators.Config;
import org.infai.ses.senergy.operators.Stream;
import org.infai.ses.senergy.utils.ConfigProvider;

public class Operator {

    public static void main(String[] args) throws Exception {
        Config config = ConfigProvider.getConfig();
        Logger.setup(config.getConfigValue("logging_level", "info"));
        InputParser inputParser = new InputParser();
        inputParser.parse(config.getInputTopicsConfigs());
        Combine combine = new Combine(
                config.getConfigValue("unique_fields", null),
                inputParser.getInputs()
        );
        Stream stream = new Stream();
        stream.start(combine);
    }
}
