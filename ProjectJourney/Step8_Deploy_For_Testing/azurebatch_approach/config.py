"""
Configure Batch and Storage Account credentials
"""
BATCH_ACCOUNT_NAME='swbatchaccount'  # Your batch account name
BATCH_ACCOUNT_KEY = 'LQbbg1fD9yne6QJsfVIEn2lsvFL6FBZq/USvpj9vA5j60gmjkPKX75BNdNuQ+BZLC1/zqhQp9/rI+ABanNwIQA=='  # Your batch account key
BATCH_ACCOUNT_URL = 'https://swbatchaccount.australiaeast.batch.azure.com'  # Your batch account URL
STORAGE_ACCOUNT_NAME = 'swbatchstoreaccount'
STORAGE_ACCOUNT_KEY = 'yRDumxSAcVK3sV/Dsq7+28gJxnG6BOUfQV5fuOHBX08wpkjUt+1FJET8YM5dA8SS4i5XudGq8EfY+ASthTdBxQ=='
STORAGE_ACCOUNT_DOMAIN = 'blob.core.windows.net' # Your storage account blob service domain

POOL_ID = 'obatchpoolid'  # Your Pool ID
POOL_NODE_COUNT = 2  # Pool node count
POOL_VM_SIZE = 'Standard_A1_v2'  # VM Type/Size
JOB_ID = 'PythonQuickstartJob'  # Job ID
STANDARD_OUT_FILE_NAME = 'stdout.txt'  # Standard Output file
