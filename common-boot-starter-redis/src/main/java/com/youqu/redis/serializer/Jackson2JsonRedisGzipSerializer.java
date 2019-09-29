package com.youqu.redis.serializer;

/*
 * Copyright 2011-2014 the original author or authors.
 * 
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * 
 *      http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

import com.fasterxml.jackson.core.JsonGenerator;
import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.core.Version;
import com.fasterxml.jackson.databind.*;
import com.fasterxml.jackson.databind.module.SimpleModule;
import com.fasterxml.jackson.databind.ser.SerializerFactory;
import com.fasterxml.jackson.databind.type.TypeFactory;
import org.apache.commons.io.IOUtils;
import org.bson.types.ObjectId;
import org.springframework.data.redis.serializer.RedisSerializer;
import org.springframework.data.redis.serializer.SerializationException;
import org.springframework.util.Assert;
import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.nio.charset.Charset;
import java.util.zip.GZIPInputStream;
import java.util.zip.GZIPOutputStream;

/**
 * {@link RedisSerializer} that can read and write JSON using <a
 * href="https://github.com/FasterXML/jackson-core">Jackson's</a> and <a
 * href="https://github.com/FasterXML/jackson-databind">Jackson Databind</a> {@link ObjectMapper}.
 * <p>
 * This converter can be used to bind to typed beans, or untyped {@link java.util.HashMap HashMap} instances.
 * <b>Note:</b>Null objects are serialized as empty arrays and vice versa.
 * 用Jackson2JsonRedisSerializer进行序列化的值，在Redis中保存的内容，比Java中多了一对双引号。
 * @author Thomas Darimont
 * @since 1.2
 */
public class Jackson2JsonRedisGzipSerializer<T> implements RedisSerializer<T> {

    public static final Charset DEFAULT_CHARSET = Charset.forName("UTF-8");

    private final JavaType javaType;

    private ObjectMapper objectMapper = createMapper();

    /**
     * Creates a new {@link Jackson2JsonRedisGzipSerializer} for the given target {@link Class}.
     *
     * @param type
     */
    public Jackson2JsonRedisGzipSerializer(Class<T> type) {
        objectMapper.configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false);
        this.javaType = getJavaType(type);
    }

    /**
     * Creates a new {@link Jackson2JsonRedisGzipSerializer} for the given target {@link JavaType}.
     *
     * @param javaType
     */
    public Jackson2JsonRedisGzipSerializer(JavaType javaType) {
        objectMapper.configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false);
        this.javaType = javaType;
    }

    @Override
    @SuppressWarnings("unchecked")
    public T deserialize(byte[] bytes) throws SerializationException {

        if (isEmpty(bytes)) {
            return null;
        }
        try {
            ByteArrayInputStream in = new ByteArrayInputStream(bytes);
            GZIPInputStream ungzip = new GZIPInputStream(in);
            byte[] data = IOUtils.toByteArray(ungzip);
            ungzip.close();
            return (T) this.objectMapper.readValue(data, 0, data.length, javaType);
        } catch (Exception ex) {
            throw new SerializationException("Could not read JSON: " + ex.getMessage(), ex);
        }
    }

    @Override
    public byte[] serialize(Object t) throws SerializationException {

        if (t == null) {
            return EMPTY_ARRAY;
        }
        try {
            byte[] bytes = this.objectMapper.writeValueAsBytes(t);

            ByteArrayOutputStream out = new ByteArrayOutputStream();
            GZIPOutputStream gzip = new GZIPOutputStream(out);
            gzip.write(bytes);
            gzip.close();
            return out.toByteArray();
        } catch (Exception ex) {
            throw new SerializationException("Could not write JSON: " + ex.getMessage(), ex);
        }
    }

    /**
     * Sets the {@code ObjectMapper} for this view. If not set, a default {@link ObjectMapper#ObjectMapper() ObjectMapper}
     * is used.
     * <p>
     * Setting a custom-configured {@code ObjectMapper} is one way to take further control of the JSON serialization
     * process. For example, an extended {@link SerializerFactory} can be configured that provides custom serializers for
     * specific types. The other option for refining the serialization process is to use Jackson's provided annotations on
     * the types to be serialized, in which case a custom-configured ObjectMapper is unnecessary.
     */
    public void setObjectMapper(ObjectMapper objectMapper) {

        Assert.notNull(objectMapper, "'objectMapper' must not be null");
        this.objectMapper = objectMapper;
    }

    /**
     * Returns the Jackson {@link JavaType} for the specific class.
     * <p>
     * Default implementation returns {@link TypeFactory#constructType(java.lang.reflect.Type)}, but this can be
     * overridden in subclasses, to allow for custom generic collection handling. For instance:
     * <p>
     * <pre class="code">
     * protected JavaType getJavaType(Class&lt;?&gt; clazz) {
     * if (List.class.isAssignableFrom(clazz)) {
     * return TypeFactory.defaultInstance().constructCollectionType(ArrayList.class, MyBean.class);
     * } else {
     * return super.getJavaType(clazz);
     * }
     * }
     * </pre>
     *
     * @param clazz the class to return the java type for
     * @return the java type
     */
    protected JavaType getJavaType(Class<?> clazz) {
        return TypeFactory.defaultInstance().constructType(clazz);
    }

    static final byte[] EMPTY_ARRAY = new byte[0];

    static boolean isEmpty(byte[] data) {
        return (data == null || data.length == 0);
    }

    private static ObjectMapper createMapper() {

        ObjectMapper mapper = new ObjectMapper();
        SimpleModule testModule = new SimpleModule("MyModule", new Version(1, 0, 0, null));
        testModule.addSerializer(ObjectId.class, _idSerializer());
        testModule.addDeserializer(ObjectId.class, _idDeserializer());
        mapper.registerModule(testModule);

        return mapper;
    }

    private static JsonSerializer<ObjectId> _idSerializer() {
        return new JsonSerializer<ObjectId>() {

            @Override
            public void serialize(ObjectId value, JsonGenerator jgen, SerializerProvider provider) throws IOException, JsonProcessingException {
                jgen.writeString(value.toString());
            }

        };
    }

    private static JsonDeserializer<ObjectId> _idDeserializer() {
        return new JsonDeserializer<ObjectId>() {

            @Override
            public ObjectId deserialize(JsonParser jp, DeserializationContext ctxt) throws IOException, JsonProcessingException {
                return new ObjectId(jp.readValueAs(String.class));
            }

        };
    }
}

