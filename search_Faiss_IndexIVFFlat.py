import json
import faiss
import logging
import argparse
import numpy as np
from clickhouse_driver import Client, errors
from typing import List, Dict, Tuple, Optional
from utils import Queries, VectorUtils, ClickHouseConnection

logging.basicConfig(level=logging.INFO)


class ClickHouseRepository:
    """
    A repository class for interacting with a ClickHouse database.
    """

    def __init__(self, connection: ClickHouseConnection):
        """Initializes the repository with an existing ClickHouse connection."""
        self.client = connection.get_client()
        self.database = connection.database

    def get_vectors(
        self, table: str, ids: str, vectors: str
    ) -> Optional[Dict[str, np.ndarray]]:
        """
        Retrieves vector data from the ClickHouse database.

        :param table: The name of the table containing vector data.
        :param ids: The column name containing document IDs.
        :param vectors: The column name containing vector data.
        :return: A dictionary mapping document IDs to NumPy vector arrays.
        """
        query = Queries.GET_VECTORS.format(
            database=self.database, table=table, ids=ids, vectors=vectors
        )

        try:
            result = self.client.execute(query)
            logging.info("Vector data retrieved successfully.")

            return {str(row[0]): np.array(row[1], dtype="float64") for row in result}

        except Exception as e:
            logging.error(f"Error retrieving vector data: {e}")
            return None


import faiss
import numpy as np
from typing import Dict, List, Tuple

class VectorSearcher:
    """
    A class for performing similarity searches using FAISS.
    """

    def __init__(self, vectors_index: Dict[str, np.ndarray], nlist: int = 100, nprobe: int = 100):
        """
        Initializes the FAISS index using IndexIVFFlat.

        :param vectors_index: A dictionary where keys are document IDs and values are vectors.
        :param nlist: The number of clusters (higher values make search faster but require tuning).
        :param nprobe: The number of clusters to search (higher values improve accuracy but slow down search).
        """
        if not vectors_index:
            raise ValueError("Vector index is empty.")

        self.doc_ids = np.array(list(vectors_index.keys()))
        self.db_vectors = np.array(list(vectors_index.values()), dtype="float32")  # FAISS uses float32

        d = self.db_vectors.shape[1]  # Dimensionality of vectors

        # Create an IVFFlat index
        quantizer = faiss.IndexFlatL2(d)  # Quantizer used for clustering
        self.index = faiss.IndexIVFFlat(quantizer, d, nlist, faiss.METRIC_L2)

        # Train the index on a subset of the data
        self.index.train(self.db_vectors)

        # Add vectors to the index
        self.index.add(self.db_vectors)

        # Set nprobe to nlist to search all clusters
        self.index.nprobe = nlist  # Ensures all clusters are searched

    def search_similar(
        self, input_vectors: List[List[float]], count: int
    ) -> Dict[int, List[Tuple[str, float]]]:
        """
        Performs a nearest-neighbor search using FAISS.

        :param input_vectors: A list of input vectors to find similar vectors for.
        :param count: The number of closest vectors to return.
        :return: A dictionary where keys are input vector indices,
                 and values are lists of tuples (document ID, distance).
        """
        similar_vectors: Dict[int, List[Tuple[str, float]]] = {}

        for idx, input_vector in enumerate(input_vectors):
            input_vector_np = np.array(input_vector, dtype="float32").reshape(1, -1)

            distances, indices = self.index.search(input_vector_np, count)

            distances = np.sqrt(distances)  # Optional: can be removed if you don't want sqrt

            similar_vectors[idx] = [
                (self.doc_ids[indices[0][i]], distances[0][i]) for i in range(count)
            ]

        return similar_vectors



def parse_arguments() -> argparse.Namespace:
    """
    Parses command-line arguments.

    :return: Parsed arguments as a namespace object.
    """
    parser = argparse.ArgumentParser(description="Vector Similarity Search")

    parser.add_argument("--host", default="localhost", help="ClickHouse host")
    parser.add_argument("--port", type=int, default=9000, help="ClickHouse port")
    parser.add_argument("-u", "--user", default="default", help="ClickHouse username")
    parser.add_argument("-p", "--password", default="", help="ClickHouse password")
    parser.add_argument(
        "--database", default="db_master", help="ClickHouse database name"
    )
    parser.add_argument("--table", default="element", help="ClickHouse table name")
    parser.add_argument("--ids", default="doc_id", help="Column name for document IDs")
    parser.add_argument(
        "--vectors", default="centroid", help="Column name for vector data"
    )
    parser.add_argument(
        "--count", type=int, default=10, help="Number of similar vectors to retrieve"
    )
    parser.add_argument(
        "--file",
        type=str,
        default="test.json",
        help="Path to input JSON file with vectors",
    )
    return parser.parse_args()

def main() -> None:
    """
    The main function that handles database connection, retrieves vector data,
    and performs similarity search.
    """
    args = parse_arguments()

    input_vectors = VectorUtils.vectors_from_json(args.file)

    try:
        connection = ClickHouseConnection(
            host=args.host,
            port=args.port,
            user=args.user,
            password=args.password,
            database=args.database,
        )

        db = ClickHouseRepository(connection)

        vectors_db = db.get_vectors(args.table, args.ids, args.vectors)

        searcher = VectorSearcher(vectors_db)
        similar_vectors = searcher.search_similar(input_vectors, args.count)

        VectorUtils.print_similar_vectors(similar_vectors)

    except Exception as e:
        logging.error(f"An error occurred: {e}")


if __name__ == "__main__":
    main()
