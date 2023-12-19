#!/bin/bash

package_name=$(find . -type f -name "alethic-ism-core.*.tar.bz2")

echo "packaging local conda channel into an artifact"
rm -rf /app/local_channel.tar.gz
tar -zcvf /app/local_channel.tar.gz /app/conda/env/local_channel


