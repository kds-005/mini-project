import dask.dataframe as dd
import pandas as pd
import numpy as np
import os
import random
from concurrent.futures import ProcessPoolExecutor

def create_employee_dataset(n_employees):
    employee_ids = np.arange(1, n_employees + 1)
    first_names = np.random.choice(["John", "Jane", "Michael", "Emily", "David", "Alice"], n_employees)
    last_names = np.random.choice(["Smith", "Johnson", "Williams", "Jones", "Brown"], n_employees)
    emails = [f"employee{i}@company.com" for i in employee_ids]
    department_ids = np.random.randint(1, 11, n_employees)  # Assuming 10 departments
    salaries = np.round(np.random.uniform(30000, 120000, n_employees), 2)
    hire_dates = pd.to_datetime(np.random.choice(pd.date_range("2023-01-01", "2023-12-31"), n_employees))
    phones = [f"+1{random.randint(1000000000, 9999999999)}" for _ in range(n_employees)]
    job_titles = np.random.choice(["Manager", "Developer", "Analyst", "Designer"], n_employees)
    is_active = np.random.choice([True, False], n_employees)
    addresses = [f"{random.randint(100, 999)} Elm St, City{random.randint(1, 100)}, State{random.randint(1, 50)}" for _ in range(n_employees)]
    
    return pd.DataFrame({
        "employee_id": employee_ids,
        "first_name": first_names,
        "last_name": last_names,
        "email": emails,
        "department_id": department_ids,
        "salary": salaries,
        "hire_date": hire_dates,
        "phone": phones,
        "job_title": job_titles,
        "is_active": is_active,
        "address": addresses,
        "created_at": pd.to_datetime("2023-01-01"),
        "updated_at": pd.to_datetime("2023-01-01"),
        # Additional 38 columns to make it 50
        **{f"custom_col_{i}": np.random.choice([None, random.randint(1, 100)], n_employees) for i in range(1, 39)}
    })

def create_department_dataset():
    departments = pd.DataFrame({
        "department_id": np.arange(1, 11),
        "department_name": np.random.choice(["HR", "IT", "Finance", "Sales", "Marketing"], 10),
        "manager_id": np.random.choice(np.arange(1, 1000001), 10),  # Foreign key to Employees
        "budget": np.round(np.random.uniform(100000, 1000000, 10), 2),
        "location": np.random.choice(["New York", "Los Angeles", "Chicago", "Houston", "Phoenix"], 10),
    })
    return departments

def create_project_dataset(n_projects):
    project_ids = np.arange(1, n_projects + 1)
    project_names = [f"Project {i}" for i in project_ids]
    department_ids = np.random.randint(1, 11, n_projects)  # Foreign key to Departments
    start_dates = pd.to_datetime(np.random.choice(pd.date_range("2023-01-01", "2023-12-31"), n_projects))
    end_dates = start_dates + pd.to_timedelta(np.random.randint(30, 365, n_projects), unit='D')
    budgets = np.round(np.random.uniform(50000, 500000, n_projects), 2)
    
    return pd.DataFrame({
        "project_id": project_ids,
        "project_name": project_names,
        "department_id": department_ids,
        "start_date": start_dates,
        "end_date": end_dates,
        "budget": budgets,
        "created_at": pd.to_datetime("2023-01-01"),
        "updated_at": pd.to_datetime("2023-01-01"),
        # Additional 42 columns to make it 50
        **{f"custom_col_{i}": np.random.choice([None, random.randint(1, 100)], n_projects) for i in range(1, 43)}
    })

def create_salary_dataset(n_salaries):
    salary_ids = np.arange(1, n_salaries + 1)
    employee_ids = np.random.choice(np.arange(1, 1000001), n_salaries)  # Foreign key to Employees
    amounts = np.round(np.random.uniform(30000, 120000, n_salaries), 2)
    effective_dates = pd.to_datetime(np.random.choice(pd.date_range("2020-01-01", "2023-01-01"), n_salaries))
    
    return pd.DataFrame({
        "salary_id": salary_ids,
        "employee_id": employee_ids,
        "amount": amounts,
        "effective_date": effective_dates,
        "created_at": pd.to_datetime("2023-01-01"),
        "updated_at": pd.to_datetime("2023-01-01"),
    })

def create_product_dataset(n_products):
    product_ids = np.arange(1, n_products + 1)
    product_names = [f"Product {i}" for i in product_ids]
    categories = np.random.choice(["Electronics", "Clothing", "Home", "Beauty"], n_products)
    prices = np.round(np.random.uniform(5.0, 500.0, n_products), 2)
    stock = np.random.randint(0, 1001, n_products)
    suppliers = np.random.choice(np.arange(1, 101), n_products)  # Foreign key to Suppliers
    product_descriptions = np.random.choice(["High quality product.", "Budget-friendly option.", "Top seller!"], n_products)
    release_dates = pd.to_datetime(np.random.choice(pd.date_range("2022-01-01", "2022-12-31"), n_products))
    warranties = np.random.choice([None, "1 Year", "2 Years"], n_products)

    return pd.DataFrame({
        "product_id": product_ids,
        "product_name": product_names,
        "category": categories,
        "price": prices,
        "stock": stock,
        "supplier_id": suppliers,
        "product_description": product_descriptions,
        "release_date": release_dates,
        "warranty": warranties,
        "created_at": pd.to_datetime("2023-01-01"),
        "updated_at": pd.to_datetime("2023-01-01"),
        # Additional 40 columns to make it 50
        **{f"custom_col_{i}": np.random.choice([None, random.randint(1, 100)], n_products) for i in range(1, 41)}
    })

def create_sales_dataset(n_sales):
    sale_ids = np.arange(1, n_sales + 1)
    employee_ids_sales = np.random.choice(np.arange(1, 1000001), n_sales)
    customer_ids_sales = np.random.randint(1, 500001, n_sales)  # Foreign key to Customers
    product_ids_sales = np.random.randint(1, 100001, n_sales)  # Foreign key to Products
    sale_dates = pd.to_datetime(np.random.choice(pd.date_range("2023-01-01", "2023-12-31"), n_sales))
    amounts = np.round(np.random.uniform(10.0, 5000.0, n_sales), 2)
    
    return pd.DataFrame({
        "sale_id": sale_ids,
        "employee_id": employee_ids_sales,
        "customer_id": customer_ids_sales,
        "product_id": product_ids_sales,
        "sale_date": sale_dates,
        "amount": amounts,
        "created_at": pd.to_datetime("2023-01-01"),
        "updated_at": pd.to_datetime("2023-01-01"),
        # Additional 2 columns to make it 10
        "payment_method": np.random.choice(["Credit Card", "Debit Card", "PayPal"], n_sales),
        "is_completed": np.random.choice([True, False], n_sales),
    })

def create_customer_dataset(n_customers):
    customer_ids = np.arange(1, n_customers + 1)
    first_names = np.random.choice(["Alice", "Bob", "Charlie", "Diana", "Eve"], n_customers)
    last_names = np.random.choice(["Smith", "Johnson", "Williams", "Jones", "Brown"], n_customers)
    emails = [f"customer{i}@mail.com" for i in customer_ids]
    phone_numbers = [f"+1{random.randint(1000000000, 9999999999)}" for _ in range(n_customers)]
    addresses = [f"{random.randint(100, 999)} Maple Ave, City{random.randint(1, 100)}, State{random.randint(1, 50)}" for _ in range(n_customers)]
    created_dates = pd.to_datetime(np.random.choice(pd.date_range("2023-01-01", "2023-12-31"), n_customers))

    return pd.DataFrame({
        "customer_id": customer_ids,
        "first_name": first_names,
        "last_name": last_names,
        "email": emails,
        "phone_number": phone_numbers,
        "address": addresses,
        "created_date": created_dates,
            # Additional 13 columns to make it 20
        **{f"custom_col_{i}": np.random.choice([None, random.randint(1, 100)], n_customers) for i in range(1, 14)}
    })

def create_transaction_dataset(n_transactions):
    transaction_ids = np.arange(1, n_transactions + 1)
    customer_ids = np.random.randint(1, 500001, n_transactions)  # Foreign key to Customers
    product_ids = np.random.randint(1, 100001, n_transactions)  # Foreign key to Products
    transaction_dates = pd.to_datetime(np.random.choice(pd.date_range("2023-01-01", "2023-12-31"), n_transactions))
    amounts = np.round(np.random.uniform(5.0, 500.0, n_transactions), 2)

    return pd.DataFrame({
        "transaction_id": transaction_ids,
        "customer_id": customer_ids,
        "product_id": product_ids,
        "transaction_date": transaction_dates,
        "amount": amounts,
        # Additional 5 columns to make it 10
        "payment_method": np.random.choice(["Credit Card", "Debit Card", "PayPal"], n_transactions),
        "status": np.random.choice(["Completed", "Pending", "Failed"], n_transactions),
        "created_at": pd.to_datetime("2023-01-01"),
        "updated_at": pd.to_datetime("2023-01-01"),
        "is_refunded": np.random.choice([True, False], n_transactions),
    })

def create_review_dataset(n_reviews):
    review_ids = np.arange(1, n_reviews + 1)
    product_ids_reviews = np.random.randint(1, 100001, n_reviews)  # Foreign key to Products
    customer_ids_reviews = np.random.randint(1, 500001, n_reviews)  # Foreign key to Customers
    review_dates = pd.to_datetime(np.random.choice(pd.date_range("2023-01-01", "2023-12-31"), n_reviews))
    ratings_reviews = np.random.randint(1, 6, n_reviews)
    review_texts = np.random.choice(["Excellent product!", "Good value.", "Not worth it.", "Loved it!", "Could be better."], n_reviews)
    is_verified = np.random.choice([True, False], n_reviews)
    helpful_count = np.random.randint(0, 101, n_reviews)

    return pd.DataFrame({
        "review_id": review_ids,
        "product_id": product_ids_reviews,
        "customer_id": customer_ids_reviews,
        "review_date": review_dates,
        "rating": ratings_reviews,
        "review_text": review_texts,
        "is_verified": is_verified,
        "helpful_count": helpful_count,
        "created_at": pd.to_datetime("2023-01-01"),
        "updated_at": pd.to_datetime("2023-01-01"),
    })

def create_inventory_dataset(n_inventory):
    inventory_ids = np.arange(1, n_inventory + 1)
    product_ids_inventory = np.random.randint(1, 100001, n_inventory)  # Foreign key to Products
    warehouse_locations = np.random.choice(["Warehouse A", "Warehouse B", "Warehouse C"], n_inventory)
    quantities_in_stock = np.random.randint(0, 501, n_inventory)
    reorder_levels = np.random.randint(50, 101, n_inventory)
    reorder_quantities = np.random.randint(10, 51, n_inventory)
    restock_dates = pd.to_datetime(np.random.choice(pd.date_range("2023-01-01", "2023-12-31"), n_inventory))

    return pd.DataFrame({
        "inventory_id": inventory_ids,
        "product_id": product_ids_inventory,
        "warehouse_location": warehouse_locations,
        "quantity_in_stock": quantities_in_stock,
        "reorder_level": reorder_levels,
        "reorder_quantity": reorder_quantities,
        "restock_date": restock_dates,
        "created_at": pd.to_datetime("2023-01-01"),
        "updated_at": pd.to_datetime("2023-01-01"),
    })

def write_to_parquet(df, dataset_name, base_directory):
    dataset_directory = os.path.join(base_directory, dataset_name)
    os.makedirs(dataset_directory, exist_ok=True)

    # Convert the DataFrame to a Dask DataFrame and write to Parquet using pyarrow
    ddf = dd.from_pandas(df, npartitions=30)  # Adjust partitions based on your memory
    ddf.to_parquet(os.path.join(dataset_directory, f"{dataset_name}.parquet"), engine='pyarrow', compression='snappy')

def create_and_write_datasets():
    base_directory = "D:/Spark get to know/resources"
    datasets_info = {
        "employees": (create_employee_dataset, 1000000),
        "departments": (create_department_dataset, None),
        "projects": (create_project_dataset, 1000000),
        "salaries": (create_salary_dataset, 1500000),
        "products": (create_product_dataset, 100000),
        "sales": (create_sales_dataset, 1000000),
        "customers": (create_customer_dataset, 5000000),
        "transactions": (create_transaction_dataset, 1000000),
        "reviews": (create_review_dataset, 2000000),
        "inventory": (create_inventory_dataset, 250000),
    }

    with ProcessPoolExecutor() as executor:
        futures = []
        for name, (func, n) in datasets_info.items():
            if n is not None:
                futures.append(executor.submit(func, n))
            else:
                futures.append(executor.submit(func))

        # Wait for all futures to complete
        for future, name in zip(futures, datasets_info.keys()):
            df = future.result()
            write_to_parquet(df, name, base_directory)

if __name__ == '__main__':
    # Create and write datasets in parallel
    create_and_write_datasets()
    print("Logical datasets created and written to Parquet successfully!")