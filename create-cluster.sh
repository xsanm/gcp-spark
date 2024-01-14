gcloud dataproc clusters create imdb-lab \
    --image-version=2.1-ubuntu20 \
    --region=us-central1 \
    --enable-component-gateway \
    --num-masters=1 \
    --master-machine-type=n2-standard-2 \
    --worker-machine-type=n2-standard-2 \
    --master-boot-disk-size=30GB \
    --worker-boot-disk-size=30GB \
    --num-workers=2 \
    --initialization-actions=gs://pyspark-fs-xsan/dataproc-init.sh \
    --metadata 'PIP_PACKAGES=mysql-connector-python' \
    --optional-components=JUPYTER



