#python ingest_data.py \
#docker build -t taxi_ingest:v001 .
docker run -it \
    --network pg-network \
    taxi_ingest:v001 \
        --user=root \
        --password=root \
        --port=5432 \
        --host=pg_database \
        --database=ny_taxi \
        --table=yellow_taxi_trips \
        --csv_url=https://github.com/DataTalksClub/nyc-tlc-data/releases/download/yellow/yellow_tripdata_2021-01.csv.gz
