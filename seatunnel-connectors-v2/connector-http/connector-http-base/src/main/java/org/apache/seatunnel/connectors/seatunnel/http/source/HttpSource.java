/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.seatunnel.connectors.seatunnel.http.source;

import org.apache.seatunnel.shade.com.typesafe.config.Config;
import org.apache.seatunnel.shade.com.typesafe.config.ConfigList;
import org.apache.seatunnel.shade.com.typesafe.config.ConfigRenderOptions;

import org.apache.seatunnel.api.common.JobContext;
import org.apache.seatunnel.api.common.PrepareFailException;
import org.apache.seatunnel.api.common.SeaTunnelAPIErrorCode;
import org.apache.seatunnel.api.serialization.DeserializationSchema;
import org.apache.seatunnel.api.source.Boundedness;
import org.apache.seatunnel.api.source.SeaTunnelSource;
import org.apache.seatunnel.api.table.catalog.CatalogTableUtil;
import org.apache.seatunnel.api.table.catalog.schema.TableSchemaOptions;
import org.apache.seatunnel.api.table.type.SeaTunnelDataType;
import org.apache.seatunnel.api.table.type.SeaTunnelRow;
import org.apache.seatunnel.api.table.type.SeaTunnelRowType;
import org.apache.seatunnel.common.config.CheckConfigUtil;
import org.apache.seatunnel.common.config.CheckResult;
import org.apache.seatunnel.common.constants.JobMode;
import org.apache.seatunnel.common.constants.PluginType;
import org.apache.seatunnel.common.exception.CommonErrorCode;
import org.apache.seatunnel.common.utils.JsonUtils;
import org.apache.seatunnel.connectors.seatunnel.common.source.AbstractSingleSplitReader;
import org.apache.seatunnel.connectors.seatunnel.common.source.AbstractSingleSplitSource;
import org.apache.seatunnel.connectors.seatunnel.common.source.SingleSplitReaderContext;
import org.apache.seatunnel.connectors.seatunnel.http.config.HttpConfig;
import org.apache.seatunnel.connectors.seatunnel.http.config.HttpParameter;
import org.apache.seatunnel.connectors.seatunnel.http.config.JsonField;
import org.apache.seatunnel.connectors.seatunnel.http.exception.HttpConnectorException;
import org.apache.seatunnel.format.json.JsonDeserializationSchema;

import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.PropertyNamingStrategy;
import com.google.auto.service.AutoService;

import java.util.ArrayList;
import java.util.List;
import java.util.Locale;
import java.util.Map;

@AutoService(SeaTunnelSource.class)
public class HttpSource extends AbstractSingleSplitSource<SeaTunnelRow> {
    protected final HttpParameter httpParameter = new HttpParameter();
    protected SeaTunnelRowType rowType;
    protected JsonField jsonField;
    protected String contentField;
    protected JobContext jobContext;
    protected DeserializationSchema<SeaTunnelRow> deserializationSchema;

    protected List<Map<String, String>> dynamicParams = new ArrayList<>();

    /** 序列化器 */
    private static final ObjectMapper OBJECT_MAPPER;

    static {
        OBJECT_MAPPER = new ObjectMapper();
        OBJECT_MAPPER.setSerializationInclusion(JsonInclude.Include.NON_NULL);
        OBJECT_MAPPER.setPropertyNamingStrategy(
                PropertyNamingStrategy.CAMEL_CASE_TO_LOWER_CASE_WITH_UNDERSCORES);
    }

    @Override
    public String getPluginName() {
        return "Http";
    }

    @Override
    public Boundedness getBoundedness() {
        return JobMode.BATCH.equals(jobContext.getJobMode())
                ? Boundedness.BOUNDED
                : Boundedness.UNBOUNDED;
    }

    @Override
    public void prepare(Config pluginConfig) throws PrepareFailException {
        CheckResult result = CheckConfigUtil.checkAllExists(pluginConfig, HttpConfig.URL.key());
        if (!result.isSuccess()) {
            throw new HttpConnectorException(
                    SeaTunnelAPIErrorCode.CONFIG_VALIDATION_FAILED,
                    String.format(
                            "PluginName: %s, PluginType: %s, Message: %s",
                            getPluginName(), PluginType.SOURCE, result.getMsg()));
        }
        this.httpParameter.buildWithConfig(pluginConfig);
        buildSchemaWithConfig(pluginConfig);
        buildDynamicParamsConfig(pluginConfig);
    }

    private void buildDynamicParamsConfig(Config pluginConfig) {
        if (pluginConfig.hasPath(HttpConfig.DYNAMIC_PARAMS.key())) {
            ConfigList dynamicConfig = pluginConfig.getList(HttpConfig.DYNAMIC_PARAMS.key());
            List<Object> configList = dynamicConfig.unwrapped();
            for (Object config : configList) {
                dynamicParams.add(
                        OBJECT_MAPPER.convertValue(
                                config, new TypeReference<Map<String, String>>() {}));
            }
        }
    }

    protected void buildSchemaWithConfig(Config pluginConfig) {
        if (pluginConfig.hasPath(TableSchemaOptions.SCHEMA.key())) {
            this.rowType = CatalogTableUtil.buildWithConfig(pluginConfig).getSeaTunnelRowType();
            // default use json format
            HttpConfig.ResponseFormat format = HttpConfig.FORMAT.defaultValue();
            if (pluginConfig.hasPath(HttpConfig.FORMAT.key())) {
                format =
                        HttpConfig.ResponseFormat.valueOf(
                                pluginConfig
                                        .getString(HttpConfig.FORMAT.key())
                                        .toUpperCase(Locale.ROOT));
            }
            switch (format) {
                case JSON:
                    this.deserializationSchema =
                            new JsonDeserializationSchema(false, false, rowType);
                    if (pluginConfig.hasPath(HttpConfig.JSON_FIELD.key())) {
                        jsonField =
                                getJsonField(pluginConfig.getConfig(HttpConfig.JSON_FIELD.key()));
                    }
                    if (pluginConfig.hasPath(HttpConfig.CONTENT_FIELD.key())) {
                        contentField = pluginConfig.getString(HttpConfig.CONTENT_FIELD.key());
                    }
                    break;
                default:
                    // TODO: use format SPI
                    throw new HttpConnectorException(
                            CommonErrorCode.ILLEGAL_ARGUMENT,
                            String.format(
                                    "Unsupported data format [%s], http connector only support json format now",
                                    format));
            }
        } else {
            this.rowType = CatalogTableUtil.buildSimpleTextSchema();
            this.deserializationSchema = new SimpleTextDeserializationSchema(this.rowType);
        }
    }

    @Override
    public void setJobContext(JobContext jobContext) {
        this.jobContext = jobContext;
    }

    @Override
    public SeaTunnelDataType<SeaTunnelRow> getProducedType() {
        return this.rowType;
    }

    @Override
    public AbstractSingleSplitReader<SeaTunnelRow> createReader(
            SingleSplitReaderContext readerContext) throws Exception {
        HttpSourceReader httpSourceReader =
                new HttpSourceReader(
                        this.httpParameter,
                        readerContext,
                        this.deserializationSchema,
                        jsonField,
                        contentField);
        httpSourceReader.setDynamicParams(dynamicParams);
        return httpSourceReader;
    }

    private JsonField getJsonField(Config jsonFieldConf) {
        ConfigRenderOptions options = ConfigRenderOptions.concise();
        return JsonField.builder()
                .fields(JsonUtils.toMap(jsonFieldConf.root().render(options)))
                .build();
    }
}
