#!/usr/bin/env bash

################################################################################
# Copyright (c) 2019 Starburst Data, Inc. All rights reserved.
#
# All information herein is owned by Starburst Data Inc. and its licensors
# ("Starburst"), if any.  This software and the concepts it embodies are
# proprietary to Starburst, are protected by trade secret and copyright law,
# and may be covered by patents in the U.S. and abroad.  Distribution,
# reproduction, and relicensing are strictly forbidden without Starburst's prior
# written permission.
#
# THIS SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
# IMPLIED.  THE WARRANTIES OF MERCHANTABILITY, FITNESS FOR A PARTICULAR PURPOSE
# AND NONINFRINGEMENT ARE EXPRESSLY DISCLAIMED. IN NO EVENT SHALL STARBURST BE
# LIABLE FOR ANY CLAIM, DAMAGES OR OTHER LIABILITY, WHETHER IN CONTRACT, TORT OR
# OTHERWISE, ARISING FROM, OUT OF OR IN CONNECTION WITH THE SOFTWARE OR ITS USE
# EVEN IF STARBURST HAS BEEN ADVISED OF THE POSSIBILITY OF SUCH DAMAGE.
#
# Please refer to your agreement(s) with Starburst for further information.
################################################################################

set -xeuo pipefail

if test $# -ne 2; then
    echo "Usage: $0 [pod_id] [coordinator|worker]" >&2
    exit 1
fi

readonly POD_ID="$1"
readonly ROLE="$2"
readonly ADDITIONAL_BOOTSTRAP_DIR="/opt/additional-bootstrap"
readonly ADDITIONAL_BOOTSTRAP="$ADDITIONAL_BOOTSTRAP_DIR/bootstrap.sh"
readonly COORDINATOR="coordinator"
readonly WORKER="worker"

if [ "$ROLE" = "$COORDINATOR" ]; then
  /usr/local/bin/presto-autoconfigure -pod-id $POD_ID -coordinator
elif [ "$ROLE" = "$WORKER" ]; then
  /usr/local/bin/presto-autoconfigure -pod-id $POD_ID
else
  echo "Unknown node type: '$ROLE'"
  exit 1
fi

if [[ -f "$ADDITIONAL_BOOTSTRAP" ]]; then
    echo "Running additional bootstrap script $ADDITIONAL_BOOTSTRAP."
    ls -alR "$ADDITIONAL_BOOTSTRAP_DIR"
    . "$ADDITIONAL_BOOTSTRAP" "$ROLE"
fi

# !!!!!!!!!!!!!!!!!!!!!!!!!!!!!
# INSERT YOUR CUSTOM STEPS HERE
# !!!!!!!!!!!!!!!!!!!!!!!!!!!!!
if [ "$ROLE" = "$COORDINATOR" ]; then
  exec /entrypoint.sh master-only --no-format &
  exec /entrypoint.sh job-master &
else
  exec /entrypoint.sh worker-only --no-format &
  exec /entrypoint.sh job-worker &
fi

if [ "$ROLE" = "$COORDINATOR" ]; then
  exec /usr/local/bin/presto-launcher -coordinator
else
  exec /usr/local/bin/presto-launcher
fi
