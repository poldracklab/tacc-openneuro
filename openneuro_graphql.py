import os.path
import os
import requests
import argparse
from dotenv import load_dotenv

OPENNEURO_URL = 'https://openneuro.org/'
DOTENV_PATH = '/corral/utexas/poldracklab/data/OpenNeuro/software/tacc-openneuro/openneuro_api_key_wbhi.env'


def graphql_query(query, openneuro_url, openneuro_api_key):
    headers = {"Content-Type": "application/json"}
    cookies = {"accessToken": openneuro_api_key}
    url = os.path.join(openneuro_url, "crn/graphql")
    json = {"query": query}
    response = requests.post(url, headers=headers, json=json, cookies=cookies)
    return response.json()

def get_ds_url(accession_number, openneuro_url, openneuro_api_key):
    query = """
    query {
        dataset(id: "accession_number") {
            worker
        }
    }
    """.replace("accession_number", accession_number)

    response_json = graphql_query(query, openneuro_url, openneuro_api_key)
    git_worker_number = str(response_json["data"]["dataset"]["worker"][-1])
    ds_url = os.path.join(openneuro_url, 'git', git_worker_number, accession_number)
    return ds_url

def new_dataset_query(openneuro_url, openneuro_api_key):
    query = """
    mutation {
      createDataset(affirmedDefaced: true) {
        id
      }
    }
    """
    
    response_json = graphql_query(query, openneuro_url, openneuro_api_key)
    accession_number = response_json["data"]["createDataset"]["id"]
    return accession_number

def snapshot(accession_number, openneuro_url, openneuro_api_key):
    query = """
    mutation {
        createSnapshot(
            datasetId: "accession_number",
            tag: "1.0.0",
            changes: ["Initial snapshot"]
        ) {
            id
        }
    }
    """.replace("accession_number", accession_number)

    return graphql_query(query, openneuro_url, openneuro_api_key)

def publish(accession_number, openneuro_url, openneuro_api_key):
    query = """
    mutation PublishDataset($datasetId: ID!) {
         publishDataset(datasetId: "accession_number")
    }
    """.replace("accession_number", accession_number)

    return graphql_query(query, openneuro_url, openneuro_api_key)

def main():
    load_dotenv(dotenv_path=DOTENV_PATH)
    openneuro_api_key = os.getenv('OPENNEURO_API_KEY')

    parser = argparse.ArgumentParser()
    parser.add_argument('-u', '--upload', action='store_true', help='Upload dataset')
    parser.add_argument('-s', '--snapshot', action='store_true', help='Create initial snapshot')
    parser.add_argument('-p', '--publish', action='store_true', help='Publish snapshot')
    parser.add_argument('-n', '--new', action='store_true', help='New dataset')
    parser.add_argument('-d', '--dataset', type=str, help='Dataset number')
    args = parser.parse_args()

    if args.upload:
        if args.new:
            accession_number = new_dataset_query(OPENNEURO_URL, openneuro_api_key)
        elif args.dataset:
            accession_number = args.dataset
        ds_url = get_ds_url(accession_number, OPENNEURO_URL, openneuro_api_key)
        print(ds_url)
    elif args.snapshot:
        response = snapshot(args.dataset, OPENNEURO_URL, openneuro_api_key)
    elif args.publish:
        response = publish(args.dataset, OPENNEURO_URL, openneuro_api_key)

    print(response)


if __name__ == "__main__":
    main()


