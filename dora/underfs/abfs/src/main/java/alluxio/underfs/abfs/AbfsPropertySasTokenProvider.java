/*
 * The Alluxio Open Foundation licenses this work under the Apache License, version 2.0
 * (the "License"). You may not use this work except in compliance with the License, which is
 * available at www.apache.org/licenses/LICENSE-2.0
 *
 * This software is distributed on an "AS IS" basis, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND,
 * either express or implied, as more fully set forth in the License.
 *
 * See the NOTICE file distributed with this work for information regarding copyright ownership.
 */

package alluxio.underfs.abfs;

import alluxio.conf.PropertyKey;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.azurebfs.extensions.SASTokenProvider;
import org.apache.hadoop.security.AccessControlException;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.Map;
import java.util.regex.Matcher;

/**
 * SasTokenProvider implementation that reads the SAS token value from the
 * Alluxio configuration.
 */
public class AbfsPropertySasTokenProvider implements SASTokenProvider {
    private static final Logger LOG = LoggerFactory.getLogger(AbfsPropertySasTokenProvider.class);

    /**
     * Configuration provided by org.apache.hadoop.fs.azurebfs.services.AbfsClient
     */
    private Configuration configuration;

    @Override
    public void initialize(Configuration conf, String accountName) throws IOException {
        LOG.debug("initialize");
        this.configuration = new Configuration(conf);
    }

    /**
     * Invokes the authorizer to obtain a SAS token.
     * SAS token is taken from the configuration where the key is of the form:
     *   fs.azure.sas.{filesystem}.{account}.dfs.core.windows.net
     *
     * @param account    the name of the storage account.
     * @param fileSystem the name of the fileSystem.
     * @param path       the file or directory path.
     * @param operation  the operation to be performed on the path.
     * @return a SAS token to perform the request operation.
     * @throws IOException            if there is a network error.
     * @throws AccessControlException if access is denied.
     */
    @Override
    public String getSASToken(String account, String fileSystem, String path, String operation)
            throws IOException, AccessControlException {
        LOG.debug("args {} {} {} {}", account, fileSystem, path, operation);

        for (Map.Entry<String, String> entry : this.configuration) {
            String key = entry.getKey();
            Object value = entry.getValue();

            Matcher matcher = PropertyKey.Template.UNDERFS_ABFS_SAS_TOKEN.match(key);
            if (matcher.matches() && matcher.groupCount() == 2) {
                String fileSystemInKey = matcher.group(1);
                String accountInKey = matcher.group(2);
                if (account.equals(accountInKey) && fileSystem.equals(fileSystemInKey)) {
                    LOG.debug("using SAS token from key {}", key);
                    return (String) value;
                }
                LOG.debug("ignoring SAS token from key {} for filesystem {} account {}",
                        key, fileSystemInKey, accountInKey);
            }
        }

        LOG.error("no SAS token found in configuration for filesystem %s on account %s", 
                    fileSystem, account);
        return null;
    }
}
