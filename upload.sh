#!/bin/bash

pushd ./archive
aws s3 cp --recursive . s3://scrape.storage.monday.health
popd