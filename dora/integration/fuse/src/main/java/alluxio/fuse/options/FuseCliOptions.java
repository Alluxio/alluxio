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

package alluxio.fuse.options;

import alluxio.AlluxioURI;
import alluxio.Constants;

import com.beust.jcommander.Parameter;
import com.beust.jcommander.ParameterException;
import com.beust.jcommander.ParametersDelegate;
import com.google.common.annotations.VisibleForTesting;

import java.nio.file.Path;
import java.util.Optional;
import javax.annotation.Nullable;

public class FuseCliOptions {
  @Parameter(
      names = {"-m", "--mount-point"},
      description = "The absolute local filesystem path that standalone Fuse will mount Alluxio "
          + "path to",
      arity = 1,
      required = true
  )
  protected Path mMountPoint;

  @Parameter(
      names = {"-u", "--root-ufs"},
      description = "The storage address of the UFS to mount to the given Fuse mount point. "
          + "All operations against the FUSE mount point "
          + "will be redirected to this storage address. "
          + "(for example, mount storage address `s3://my_bucket/my_folder` "
          + "to local FUSE mount point `/mnt/alluxio-fuse`; "
          + "local operations like `mkdir /mnt/alluxio-fuse/folder` will be translated to "
          + "`mkdir s3://my_bucket/my_folder/folder`)",
      arity = 1,
      required = false,
      converter = UfsUriOptionConverter.class
  )
  @Nullable
  protected AlluxioURI mRootUfsUri;

  @ParametersDelegate
  @Nullable
  protected MountCliOptions mMountCliOptions;

  @Parameter(
      names = {"--update-check"},
      description = "Enables or disables the FUSE version update check. "
          + "Disabled by default when connecting to Alluxio system cache or Dora cache. "
          + "Enabled by default when connecting an under storage directly.",
      arity = 0,
      required = false,
      hidden = true
  )
  @Nullable
  protected Boolean mUpdateCheck = null;

  @Parameter(
      names = {"-h", "--help"},
      description = "Display this help message",
      help = true,
      arity = 0,
      required = false
  )
  protected boolean mHelp = false;

  // Though this converts to an AlluxioURI, it's actually a UFS URI, because life is a lie :-)
  private class UfsUriOptionConverter extends BaseValueConverter<AlluxioURI> {
    UfsUriOptionConverter(String optionName) {
      super(optionName);
    }

    @Override
    public AlluxioURI convert(String value) {
      AlluxioURI ufsUri = new AlluxioURI(value);
      if (!ufsUri.hasScheme()) {
        throw new ParameterException(getErrorString(value, "a UFS URI", "no scheme"));
      }
      if (Constants.SCHEME.equals(ufsUri.getScheme())) {
        throw new ParameterException(getErrorString(value, "a UFS URI", "the scheme is "
            + Constants.SCHEME));
      }
      return ufsUri;
    }
  }

  public Optional<Path> getMountPoint() {
    return Optional.ofNullable(mMountPoint);
  }

  public Optional<AlluxioURI> getRootUfsUri() {
    return Optional.ofNullable(mRootUfsUri);
  }

  public Optional<Boolean> getUpdateCheck() {
    return Optional.ofNullable(mUpdateCheck);
  }

  public Optional<Boolean> getHelp() {
    return Optional.ofNullable(mHelp);
  }

  /**
   * Used only for testing. For convenient access to mount options, use
   * {@link #getMountOptions()}.
   *
   * @return mount cli options
   */
  @VisibleForTesting
  Optional<MountCliOptions> getMountCliOptions() {
    return Optional.ofNullable(mMountCliOptions);
  }

  public Optional<MountOptions> getMountOptions() {
    return getMountCliOptions().map(MountCliOptions::getMountOptions);
  }
}
