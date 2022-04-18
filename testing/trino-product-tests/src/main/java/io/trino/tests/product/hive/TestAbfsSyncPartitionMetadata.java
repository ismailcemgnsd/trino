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
package io.trino.tests.product.hive;

import org.testng.annotations.Parameters;

import static io.trino.tests.product.hive.util.TemporaryHiveTable.randomTableSuffix;
import static java.lang.String.format;
import static java.util.Objects.requireNonNull;

public class TestAbfsSyncPartitionMetadata
        extends AbstractTestSyncPartitionMetadata
{
    private final String container;
    private final String account;
    private final String schema = "test_" + randomTableSuffix();

    @Parameters({
            "hive.hadoop2.azure-abfs-container",
            "hive.hadoop2.azure-abfs-account",
    })
    public TestAbfsSyncPartitionMetadata(String container, String account)
    {
        this.container = requireNonNull(container, "container is null");
        this.account = requireNonNull(account, "account is null");
    }

    @Override
    protected String schemaLocation()
    {
        return format("abfs://%s@%s.dfs.core.windows.net/%s", container, account, schema);
    }
}
