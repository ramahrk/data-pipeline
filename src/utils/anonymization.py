import hashlib
from datetime import datetime

from src.utils.data_access import find_customer_by_id, find_customers_by_email
from src.utils.logging import setup_logger

# Set up logger
logger = setup_logger(__name__)


def anonymize_customer_data(customer_id=None, email=None):
    """
    Anonymize customer data based on customer_id or email.

    Args:
        customer_id (str, optional): Customer ID to anonymize
        email (str, optional): Email to anonymize

    Returns:
        int: Number of records anonymized
    """
    if not customer_id and not email:
        logger.warning("No customer_id or email provided for anonymization")
        return 0

    count = 0

    # Find customer records to anonymize
    customers_to_anonymize = []

    if customer_id:
        customer = find_customer_by_id(customer_id)
        if customer:
            customers_to_anonymize.append(customer)

    if email:
        email_customers = find_customers_by_email(email)
        if email_customers:
            customers_to_anonymize.extend(email_customers)

    # Deduplicate customers by ID
    unique_customers = {}
    for customer in customers_to_anonymize:
        if "id" in customer:
            unique_customers[customer["id"]] = customer

    # Perform anonymization
    for customer_id, customer in unique_customers.items():
        try:
            # Generate anonymized values
            anon_email = generate_anonymous_email(customer_id)
            anon_first_name = "ANONYMIZED"
            anon_last_name = "ANONYMIZED"

            # Update customer record
            customer["email"] = anon_email
            customer["first_name"] = anon_first_name
            customer["last_name"] = anon_last_name

            # Add anonymization metadata
            customer["_anonymized"] = True
            customer["_anonymized_at"] = datetime.now().isoformat()

            # Update the customer record
            if update_customer(customer):
                count += 1
                logger.info(f"Successfully anonymized customer: {customer_id}")
            else:
                logger.error(f"Failed to update anonymized customer: {customer_id}")

        except Exception as e:
            logger.error(f"Error anonymizing customer {customer_id}: {str(e)}")

    return count


def generate_anonymous_email(identifier):
    """
    Generate an anonymous email that is consistent for the same identifier.

    Args:
        identifier (str): Unique identifier to base the email on

    Returns:
        str: Anonymized email address
    """
    # Create a hash of the identifier for consistency
    hash_obj = hashlib.sha256(str(identifier).encode())
    hash_hex = hash_obj.hexdigest()[:8]

    return f"anon_{hash_hex}@example.com"


def update_customer(customer):
    """
    Update a customer record in the reference data.

    Args:
        customer (dict): Updated customer data

    Returns:
        bool: True if successful, False otherwise
    """
    if "id" not in customer:
        logger.error("Cannot update customer without ID")
        return False

    import json
    import os

    from src.config import REFERENCE_DATA_PATH

    reference_dir = os.path.join(REFERENCE_DATA_PATH, "customers")
    if not os.path.exists(reference_dir):
        os.makedirs(reference_dir, exist_ok=True)

    customer_file = os.path.join(reference_dir, f"{customer['id']}.json")

    with open(customer_file, "w") as f:
        json.dump(customer, f)

    return True
