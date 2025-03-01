/*
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
package io.trino.plugin.exchange.s3;

import com.google.common.collect.ImmutableMap;
import io.airlift.units.DataSize;
import org.testng.annotations.Test;

import java.util.Map;

import static io.airlift.configuration.testing.ConfigAssertions.assertFullMapping;
import static io.airlift.configuration.testing.ConfigAssertions.assertRecordedDefaults;
import static io.airlift.configuration.testing.ConfigAssertions.recordDefaults;
import static io.airlift.units.DataSize.Unit.MEGABYTE;
import static software.amazon.awssdk.services.s3.model.StorageClass.REDUCED_REDUNDANCY;
import static software.amazon.awssdk.services.s3.model.StorageClass.STANDARD;

public class TestExchangeS3Config
{
    @Test
    public void testDefaults()
    {
        assertRecordedDefaults(recordDefaults(ExchangeS3Config.class)
                .setS3AwsAccessKey(null)
                .setS3AwsSecretKey(null)
                .setS3Region(null)
                .setS3Endpoint(null)
                .setS3UseWebIdentityTokenCredentials(false)
                .setS3MaxErrorRetries(10)
                .setS3UploadPartSize(DataSize.of(5, MEGABYTE))
                .setStorageClass(STANDARD)
                .setAsyncClientConcurrency(500));
    }

    @Test
    public void testExplicitPropertyMappings()
    {
        Map<String, String> properties = ImmutableMap.<String, String>builder()
                .put("exchange.s3.aws-access-key", "access")
                .put("exchange.s3.aws-secret-key", "secret")
                .put("exchange.s3.region", "us-west-1")
                .put("exchange.s3.endpoint", "https://s3.us-east-1.amazonaws.com")
                .put("exchange.s3.use-web-identity-token-credentials", "true")
                .put("exchange.s3.max-error-retries", "8")
                .put("exchange.s3.upload.part-size", "10MB")
                .put("exchange.s3.storage-class", "REDUCED_REDUNDANCY")
                .put("exchange.s3.async-client-concurrency", "202")
                .buildOrThrow();

        ExchangeS3Config expected = new ExchangeS3Config()
                .setS3AwsAccessKey("access")
                .setS3AwsSecretKey("secret")
                .setS3Region("us-west-1")
                .setS3Endpoint("https://s3.us-east-1.amazonaws.com")
                .setS3UseWebIdentityTokenCredentials(true)
                .setS3MaxErrorRetries(8)
                .setS3UploadPartSize(DataSize.of(10, MEGABYTE))
                .setStorageClass(REDUCED_REDUNDANCY)
                .setAsyncClientConcurrency(202);

        assertFullMapping(properties, expected);
    }
}
