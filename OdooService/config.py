"""
Donna AI Agent - Configuration
Centralized configuration for Odoo, OpenAI, and other services.
"""
import os


class Config:
    # Odoo Settings
    ODOO_URL = os.getenv("ODOO_URL", "https://dkm-customs.odoo.com")
    ODOO_DB = os.getenv("ODOO_DB", "vva-onniti-dkm-main-20654023")
    ODOO_USERNAME = os.getenv("ODOO_USERNAME", "anas.benabbou@dkm-customs.com")
    ODOO_API_KEY = os.getenv("ODOO_API_KEY", "a3d96d0d41b4d1ac3ab1d5cc91d33e7fc0611946")

    # Memory Service (Pinecone)
    # Using the same key provided for DonnaMemory
    PINECONE_API_KEY = os.getenv("PINECONE_API_KEY", "pcsk_2vqLqd_GwCV25x1xrWxWvNR2R7rcZkaj7u8Qhfo95cW2A7G5gsVFGK8aiCSgbCrskkNFew")
    MEMORY_SERVICE_URL = os.getenv("MEMORY_SERVICE_URL", "http://localhost:7073/api/DonnaMemory")
