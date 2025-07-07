import argparse
import gzip
import json
import os
from datetime import datetime, timedelta


def extract_erasure_requests(file_path):
    """Extract erasure requests from a gzipped JSONL file."""
    requests = []
    try:
        with gzip.open(file_path, "rt") as f:
            for line in f:
                request = json.loads(line)
                customer_id = request.get("customer-id")
                email = request.get("email")
                if customer_id or email:
                    requests.append((customer_id, email))
        return requests
    except Exception as e:
        print(f"Error reading {file_path}: {e}")
        return []


def create_data_for_date(erasure_date, prev_date):
    """Create customer, product, and transaction data for a given date."""
    erasure_file = f"data/input/date={erasure_date}/hour=00/erasure-requests.json.gz"
    if not os.path.exists(erasure_file):
        print(f"Erasure file not found: {erasure_file}")
        return

    erasure_requests = extract_erasure_requests(erasure_file)
    if not erasure_requests:
        print(f"No erasure requests found in {erasure_file}")
        return

    print(f"Found {len(erasure_requests)} erasure requests for {erasure_date}")

    customer_dir = f"data/input/date={prev_date}/hour=00"
    os.makedirs(customer_dir, exist_ok=True)

    customers = []
    for i, (customer_id, email) in enumerate(erasure_requests):
        cid = customer_id or f"generated-{i+1}"
        em = email or f"user{i+1}@example.com"
        customers.append(
            {
                "id": cid,
                "first_name": f"Test{i+1}",
                "last_name": f"User{i+1}",
                "email": em,
            }
        )

    for i in range(3):
        customers.append(
            {
                "id": f"non-match-{i+1}",
                "first_name": f"NonMatch{i+1}",
                "last_name": f"User{i+1}",
                "email": f"nonmatch{i+1}@example.com",
            }
        )

    customer_file = f"{customer_dir}/customers.json.gz"
    with gzip.open(customer_file, "wt") as f:
        for customer in customers:
            f.write(json.dumps(customer) + "\n")

    print(f"Created {len(customers)} customer records in {customer_file}")

    products = [
        {
            "sku": f"PROD-{prev_date}-1",
            "name": "Test Product 1",
            "price": 19.99,
            "popularity": 4.5,
        },
        {
            "sku": f"PROD-{prev_date}-2",
            "name": "Test Product 2",
            "price": 29.99,
            "popularity": 3.8,
        },
    ]

    product_file = f"{customer_dir}/products.json.gz"
    with gzip.open(product_file, "wt") as f:
        for product in products:
            f.write(json.dumps(product) + "\n")

    print(f"Created {len(products)} product records in {product_file}")

    transactions = []
    for i, customer in enumerate(customers):
        transactions.append(
            {
                "transaction_id": f"TRX-{prev_date}-{i+1}",
                "customer_id": customer["id"],
                "purchases": {
                    "products": [
                        {
                            "sku": products[i % 2]["sku"],
                            "quantity": i + 1,
                            "price": str(products[i % 2]["price"]),
                            "total": str((i + 1) * products[i % 2]["price"]),
                        }
                    ],
                    "total_cost": str((i + 1) * products[i % 2]["price"]),
                },
            }
        )

    transaction_file = f"{customer_dir}/transactions.json.gz"
    with gzip.open(transaction_file, "wt") as f:
        for transaction in transactions:
            f.write(json.dumps(transaction) + "\n")

    print(f"Created {len(transactions)} transaction records in {transaction_file}")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Create matching customer data.")
    parser.add_argument(
        "--dates",
        nargs="+",
        help="Dates to generate data for (YYYY-MM-DD). If not provided, default dates will be used.",
    )
    args = parser.parse_args()

    if args.dates:
        dates_with_erasure = args.dates
    else:
        dates_with_erasure = [
            "2020-01-25",
            "2020-01-27",
            "2020-01-28",
            "2020-01-29",
            "2020-01-30",
            "2020-01-31",
            "2020-02-01",
        ]

    for erasure_date in dates_with_erasure:
        date_obj = datetime.strptime(erasure_date, "%Y-%m-%d")
        prev_date_obj = date_obj - timedelta(days=1)
        prev_date = prev_date_obj.strftime("%Y-%m-%d")
        create_data_for_date(erasure_date, prev_date)

    print("\nCreated matching customer data for all dates with erasure requests.")
    print(
        "You can now run the ETL pipeline on these dates to test erasure functionality."
    )
