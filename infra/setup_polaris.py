import os
import time
import logging
import requests
from dotenv import load_dotenv

load_dotenv()

POLARIS_CATALOG_URL = "http://polaris:8181/api/catalog"
POLARIS_MGMT_URL    = "http://polaris:8181/api/management/v1"
CLIENT_ID           = "root"
CLIENT_SECRET       = os.getenv("POLARIS_BOOTSTRAP_CREDENTIALS", "").split(",")[-1]
MINIO_BUCKET        = os.getenv("MINIO_BUCKET_PROD")
MINIO_ENDPOINT      = "http://minio:9000"
WAREHOUSE           = "chess_warehouse"
NAMESPACE           = "prod"

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)-8s %(message)s", datefmt="%Y-%m-%d %H:%M:%S")
logger = logging.getLogger(__name__)


def wait_for_polaris():
    for i in range(30):
        try:
            r = requests.get(f"{POLARIS_CATALOG_URL}/v1/config", timeout=5)
            if r.status_code in [200, 401, 403]:
                logger.info("Polaris is ready")
                return True
        except Exception:
            pass
        logger.info(f"Waiting for Polaris... ({i+1}/30)")
        time.sleep(2)
    return False


def get_token() -> str:
    r = requests.post(
        f"{POLARIS_CATALOG_URL}/v1/oauth/tokens",
        data={"grant_type": "client_credentials", "client_id": CLIENT_ID,
              "client_secret": CLIENT_SECRET, "scope": "PRINCIPAL_ROLE:ALL"}
    )
    r.raise_for_status()
    logger.info("✓ Authenticated")
    return r.json()["access_token"]


def hdrs(token: str) -> dict:
    return {"Authorization": f"Bearer {token}", "Content-Type": "application/json"}


CATALOG_PAYLOAD = {
    "type": "INTERNAL",
    "name": WAREHOUSE,
    "properties": {
        "default-base-location": f"s3://{MINIO_BUCKET}/iceberg",
        "allow-external-table-read-delegation": "true",
        "s3.path-style-access": "true",
    },
    "storageConfigInfo": {
        "storageType": "S3",
        "allowedLocations": [f"s3://{MINIO_BUCKET}"],
        "endpoint": MINIO_ENDPOINT,
        "pathStyleAccess": True,
        "region": "us-east-1",
        "roleArn": "arn:aws:iam::000000000000:role/polaris",
    }
}


def create_catalog(token: str):
    r = requests.post(f"{POLARIS_MGMT_URL}/catalogs", headers=hdrs(token),
                      json={"catalog": CATALOG_PAYLOAD})
    if r.status_code == 409:
        existing = requests.get(f"{POLARIS_MGMT_URL}/catalogs/{WAREHOUSE}", headers=hdrs(token)).json()
        version  = existing.get("entityVersion", 1)
        requests.put(f"{POLARIS_MGMT_URL}/catalogs/{WAREHOUSE}", headers=hdrs(token), json={
            "currentEntityVersion": version,
            "properties": CATALOG_PAYLOAD["properties"],
            "storageConfigInfo": CATALOG_PAYLOAD["storageConfigInfo"],
        })
        logger.info(f"✓ Catalog '{WAREHOUSE}' updated")
    else:
        r.raise_for_status()
        logger.info(f"✓ Created catalog '{WAREHOUSE}'")


def create_namespace(token: str):
    r = requests.post(f"{POLARIS_CATALOG_URL}/v1/{WAREHOUSE}/namespaces",
                      headers=hdrs(token), json={"namespace": [NAMESPACE]})
    if r.status_code in [409, 400]:
        logger.info(f"✓ Namespace '{NAMESPACE}' already exists")
    else:
        r.raise_for_status()
        logger.info(f"✓ Created namespace '{NAMESPACE}'")


def create_table(token: str, name: str, schema: dict):
    r = requests.post(
        f"{POLARIS_CATALOG_URL}/v1/{WAREHOUSE}/namespaces/{NAMESPACE}/tables",
        headers=hdrs(token),
        json={
            "name": name,
            "schema": schema,
            "location": f"s3://{MINIO_BUCKET}/iceberg/{NAMESPACE}/{name}",
            "properties": {
                "write.format.default": "parquet",
                "write.parquet.compression-codec": "zstd",
                "s3.path-style-access": "true",
                "s3.endpoint": MINIO_ENDPOINT,
            }
        }
    )
    if r.status_code in [409, 200]:
        logger.info(f"✓ Table '{NAMESPACE}.{name}' already exists")
    elif r.status_code == 500 and "already exists" in r.text.lower():
        logger.info(f"✓ Table '{NAMESPACE}.{name}' already exists")
    elif r.status_code == 200:
        logger.info(f"✓ Created table '{NAMESPACE}.{name}'")
    else:
        logger.error(f"Table creation failed: {r.status_code} {r.text}")
        r.raise_for_status()


def create_principal(token: str, name: str):
    r = requests.post(f"{POLARIS_MGMT_URL}/principals", headers=hdrs(token),
                      json={"principal": {"name": name, "type": "SERVICE"}})
    if r.status_code == 201:
        data = r.json()
        creds = data.get("credentials") or data.get("principal", {}).get("credentials", {})
        logger.info(f"✓ Created principal '{name}'")
        return creds.get("clientId"), creds.get("clientSecret")
    elif r.status_code == 409:
        logger.info(f"✓ Principal '{name}' exists — rotating credentials")
        r2 = requests.post(f"{POLARIS_MGMT_URL}/principals/{name}/rotate-credentials", headers=hdrs(token))
        if r2.status_code == 200:
            c = r2.json()
            return c.get("clientId"), c.get("clientSecret")
    logger.error(f"Failed to create principal '{name}': {r.text}")
    return None, None


def create_catalog_role(token: str, role: str):
    r = requests.post(f"{POLARIS_MGMT_URL}/catalogs/{WAREHOUSE}/catalog-roles",
                      headers=hdrs(token), json={"catalogRole": {"name": role}})
    if r.status_code in [200, 201, 409]:
        logger.info(f"✓ Catalog role '{role}' ready")


def grant_privilege(token: str, role: str, grant: dict):
    r = requests.put(
        f"{POLARIS_MGMT_URL}/catalogs/{WAREHOUSE}/catalog-roles/{role}/grants",
        headers=hdrs(token), json={"grant": grant}
    )
    if r.status_code not in [200, 201]:
        logger.warning(f"Grant failed for {role}: {r.status_code} {r.text}")


def link_principal_to_catalog_role(token: str, principal: str, catalog_role: str):
    p_role = f"{principal}_role"
    requests.post(f"{POLARIS_MGMT_URL}/principal-roles", headers=hdrs(token),
                  json={"principalRole": {"name": p_role}})
    requests.put(f"{POLARIS_MGMT_URL}/principals/{principal}/principal-roles",
                 headers=hdrs(token), json={"principalRole": {"name": p_role}})
    r = requests.put(f"{POLARIS_MGMT_URL}/principal-roles/{p_role}/catalog-roles/{WAREHOUSE}",
                     headers=hdrs(token), json={"catalogRole": {"name": catalog_role}})
    if r.status_code in [200, 201]:
        logger.info(f"✓ Linked '{principal}' → '{catalog_role}'")


def setup_principal(token: str, name: str, role: str, grants: list):
    cid, csecret = create_principal(token, name)
    create_catalog_role(token, role)
    for g in grants:
        grant_privilege(token, role, g)
    link_principal_to_catalog_role(token, name, role)
    return cid, csecret


GAME_START_SCHEMA = {
    "type": "struct", "schema-id": 0,
    "fields": [
        {"id": 1,  "name": "game_id",      "required": True,  "type": "string"},
        {"id": 2,  "name": "timestamp",    "required": True,  "type": "string"},
        {"id": 3,  "name": "speed",        "required": False, "type": "string"},
        {"id": 4,  "name": "rated",        "required": False, "type": "boolean"},
        {"id": 5,  "name": "variant",      "required": False, "type": "string"},
        {"id": 6,  "name": "white_id",     "required": False, "type": "string"},
        {"id": 7,  "name": "white_rating", "required": False, "type": "int"},
        {"id": 8,  "name": "white_title",  "required": False, "type": "string"},
        {"id": 9,  "name": "black_id",     "required": False, "type": "string"},
        {"id": 10, "name": "black_rating", "required": False, "type": "int"},
        {"id": 11, "name": "black_title",  "required": False, "type": "string"},
        {"id": 12, "name": "source",       "required": False, "type": "string"},
        {"id": 13, "name": "tournament_id","required": False, "type": "string"},
    ]
}

MOVES_SCHEMA = {
    "type": "struct", "schema-id": 0,
    "fields": [
        {"id": 1,  "name": "game_id",       "required": True,  "type": "string"},
        {"id": 2,  "name": "timestamp",      "required": True,  "type": "string"},
        {"id": 3,  "name": "move",           "required": False, "type": "string"},
        {"id": 4,  "name": "fen",            "required": False, "type": "string"},
        {"id": 5,  "name": "white_clock",    "required": False, "type": "int"},
        {"id": 6,  "name": "black_clock",    "required": False, "type": "int"},
        {"id": 7,  "name": "move_number",    "required": False, "type": "int"},
        {"id": 8,  "name": "game_phase",     "required": False, "type": "string"},
        {"id": 9,  "name": "time_spent_s",   "required": False, "type": "float"},
        {"id": 10, "name": "time_pressure",  "required": False, "type": "boolean"},
        {"id": 11, "name": "is_check",       "required": False, "type": "boolean"},
        {"id": 12, "name": "eval_delta",     "required": False, "type": "float"},
        {"id": 13, "name": "classification", "required": False, "type": "string"},
        {"id": 14, "name": "best_move",      "required": False, "type": "string"},
    ]
}

GAME_END_SCHEMA = {
    "type": "struct", "schema-id": 0,
    "fields": [
        {"id": 1, "name": "game_id",   "required": True,  "type": "string"},
        {"id": 2, "name": "timestamp", "required": True,  "type": "string"},
        {"id": 3, "name": "winner",    "required": False, "type": "string"},
        {"id": 4, "name": "status",    "required": False, "type": "string"},
    ]
}


def main():
    if not wait_for_polaris():
        logger.error("Polaris not ready — aborting")
        return 1

    token = get_token()

    create_catalog(token)
    create_namespace(token)

    create_table(token, "game_start", GAME_START_SCHEMA)
    create_table(token, "moves",      MOVES_SCHEMA)
    create_table(token, "game_end",   GAME_END_SCHEMA)

    etl_id, etl_secret = setup_principal(token, "airflow_etl", "catalog_admin", [
        {"type": "catalog",   "privilege": "CATALOG_MANAGE_CONTENT"},
        {"type": "namespace", "namespace": [NAMESPACE], "privilege": "NAMESPACE_FULL_METADATA"},
    ])

    sr_id, sr_secret = setup_principal(token, "starrocks_query", "catalog_reader", [
        {"type": "catalog",   "privilege": "CATALOG_MANAGE_METADATA"},
        {"type": "catalog",   "privilege": "TABLE_READ_DATA"},
        {"type": "catalog",   "privilege": "TABLE_LIST"},
        {"type": "namespace", "namespace": [NAMESPACE], "privilege": "NAMESPACE_LIST"},
    ])

    logger.info("\n✓ Polaris setup complete")
    if etl_id:
        logger.info(f"  airflow_etl     → id: {etl_id}  secret: {etl_secret}")
    if sr_id:
        logger.info(f"  starrocks_query → id: {sr_id}   secret: {sr_secret}")
    logger.info("\nUpdate StarRocks catalog credential with starrocks_query id:secret")


if __name__ == "__main__":
    main()
