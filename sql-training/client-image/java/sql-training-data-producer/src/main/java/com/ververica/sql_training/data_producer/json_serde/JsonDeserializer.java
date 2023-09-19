/*
 * Copyright 2019 Ververica GmbH
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *  http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.ververica.sql_training.data_producer.json_serde;

import com.fasterxml.jackson.databind.ObjectMapper;

import java.io.IOException;

/**
 * Deserializes a record from a JSON string.
 *
 * @param <T> The type of the deserialized record.
 */
public class JsonDeserializer<T> {

    private final Class<T> recordClazz;
    private final ObjectMapper jsonMapper;

    public JsonDeserializer(Class<T> recordClazz) {
        this.recordClazz = recordClazz;
        this.jsonMapper = new ObjectMapper();
    }

    public T parseFromString(String line) {
        try {
            return jsonMapper.readValue(line, this.recordClazz);
        } catch (IOException e) {
            throw new IllegalArgumentException("Could not deserialize record: " + line + " as class " + recordClazz, e);
        }
    }
}
