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

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;

/**
 * Serializes a record as JSON string.
 *
 * @param <T> The type for the records to serialize.
 */
public class JsonSerializer<T> {

    private final ObjectMapper jsonMapper = new ObjectMapper();

    public String toJSONString(T r) {
        try {
            return jsonMapper.writeValueAsString(r);
        } catch (JsonProcessingException e) {
            throw new IllegalArgumentException("Could not serialize record: " + r, e);
        }
    }

    public byte[] toJSONBytes(T r) {
        try {
            return jsonMapper.writeValueAsBytes(r);
        } catch (JsonProcessingException e) {
            throw new IllegalArgumentException("Could not serialize record: " + r, e);
        }
    }
}
