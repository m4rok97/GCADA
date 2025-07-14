# GCADA

GCADA (Graph Community based Anomaly Detection Algorithms) is a framework for detecting anomalies in graph data. It combines community detection, feature selection and a set of anomaly scoring methods. Example datasets and cached computation results are bundled in `datasets/` and `cache/`.

## Requirements

Install Python dependencies using `pip`:

```bash
pip install -r requirements.txt
```

The project was tested with Python 3.10.

## Running the framework

The simplest way to run the detection pipeline on the provided Disney/Amazon dataset is:

```bash
python main.py
```

This will load the graph, compute communities, select informative features with the Laplacian method and run the **glance** anomaly detector. Results are written to `anomalies_disney_amazon_glance.json`.

### Using PySpark

For large graphs PySpark can be used to parallelise the operations. Execute:

```bash
python spark_main.py
```

which performs the same pipeline but using Spark's distributed primitives.

## Datasets

The repository currently includes the `disney_amazon` graph located under `datasets/`. `GraphManager` supports additional dataset names (such as Bitcoin networks or US election retweets) if the corresponding files are placed under the expected paths.

## Caches

Community detection can be expensive. Precomputed communities are stored under the `cache/` directory. When the framework is executed these files are reused when available.

