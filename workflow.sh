gcloud beta dataproc workflow-templates create imdb-flow --region us-central1

gcloud dataproc workflow-templates \
    set-cluster-selector imdb-flow \
    --region=us-central1 \
    --cluster-labels=goog-dataproc-cluster-name=imdb-lab

gcloud dataproc workflow-templates add-job pyspark gs://pyspark-fs-xsan/extraction_pipeline.py \
    --region=us-central1 \
    --step-id=extraction-pipeline \
    --workflow-template=imdb-flow

gcloud dataproc workflow-templates add-job pyspark gs://pyspark-fs-xsan/transform1.py \
    --region=us-central1 \
    --step-id=flow1 \
    --start-after=extraction-pipeline \
    --workflow-template=imdb-flow

gcloud dataproc workflow-templates add-job pyspark gs://pyspark-fs-xsan/transform2.py \
    --region=us-central1 \
    --step-id=flow2 \
    --start-after=extraction-pipeline \
    --workflow-template=imdb-flow

gcloud dataproc workflow-templates instantiate imdb-flow --region us-central1



