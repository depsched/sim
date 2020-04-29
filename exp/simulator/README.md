## Simulator

## Input files: 

in_ecr_image_list.csv: image urls located at AWS ECR repos
in_image_stats.csv: star count, pull count, size statistics of docker official images
in_layer_stats.csv: star count, pull count, size statistics derived from in_image_stats.csv
in_layer_pull_stats.csv: reports download latency and registration latency of each layer

## Output files:

out_image_pull_stats.csv

## Dependencies:


