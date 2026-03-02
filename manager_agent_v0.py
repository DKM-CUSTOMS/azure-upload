import os
import sys
import json
import logging
from openai import OpenAI
from azure.identity import DefaultAzureCredential
from azure.keyvault.secrets import SecretClient
from azure.storage.blob import BlobServiceClient

# Suppress noisy Azure SDK logs
logging.getLogger("azure.core.pipeline.policies.http_logging_policy").setLevel(logging.WARNING)
logging.getLogger("azure.identity").setLevel(logging.WARNING)

KEY_VAULT_URL = "https://kv-functions-python.vault.azure.net"
SECRET_NAME_OPENAI = "OPENAI-API-KEY"
SECRET_NAME_BLOB = "azure-storage-account-access-key2"
CONTAINER_NAME = "document-intelligence"

PLAYBOOK = [
    {"action": "PRIORITIZE_HIGH_VOLUME", "description": "Suggest prioritizing high-volume declarants to clear backlogs."},
    {"action": "ASSIGN_SENIOR_REVIEW", "description": "Assign senior review for high-complexity cases (e.g., cases with many modifications)."},
    {"action": "REDUCE_INTERRUPTIONS", "description": "Reduce interruptions by batching communications or queries."},
    {"action": "REBALANCE_WORKLOAD", "description": "Suggest moving some work from an overloaded user to a less loaded user IN THE SAME TEAM."}
]

class BlobLoader:
    def __init__(self):
        print("[System] Connecting to Azure Key Vault & Blob Storage...")
        try:
            credential = DefaultAzureCredential()
            kv_client = SecretClient(vault_url=KEY_VAULT_URL, credential=credential)
            
            self.openai_api_key = kv_client.get_secret(SECRET_NAME_OPENAI).value
            blob_conn_string = kv_client.get_secret(SECRET_NAME_BLOB).value
            
            self.blob_service_client = BlobServiceClient.from_connection_string(blob_conn_string)
            self.container_client = self.blob_service_client.get_container_client(CONTAINER_NAME)
            print("[System] Connected successfully.")
        except Exception as e:
            print(f"[Error] Failed to initialize clients: {e}")
            sys.exit(1)

    def get_json(self, blob_path):
        try:
            blob_client = self.container_client.get_blob_client(blob_path)
            if not blob_client.exists():
                return None
            data = blob_client.download_blob().readall()
            return json.loads(data)
        except Exception as e:
            print(f"[Error] Reading {blob_path}: {e}")
            return None

class MetricsEngine:
    def __init__(self, ten_day_summary):
        self.ten_day = ten_day_summary or {}
        
    def get_team_rankings(self, top_k=5):
        """Builds a deterministic fact sheet from the 10-day summary cache."""
        facts = {}
        
        try:
            if isinstance(self.ten_day, dict):
                # Try to extract users and sort by total_files_handled if present
                user_list = []
                # Fallback if the data is inside a key like 'users' or 'data'
                users_data = self.ten_day.get("users", self.ten_day.get("data", self.ten_day)) 
                
                if isinstance(users_data, dict):
                    for username, stats in users_data.items():
                        if isinstance(stats, dict):
                            # Guessing common metric names based on V2/V3
                            vol = stats.get("total_files_handled", stats.get("total_handled", stats.get("total", 0)))
                            mods = stats.get("total_modifications", stats.get("modifications", 0))
                            user_list.append({"user": username, "volume": vol, "modifications": mods})
                            
                elif isinstance(users_data, list):
                    for user_obj in users_data:
                        u = user_obj.get("username", user_obj.get("user", "Unknown"))
                        vol = user_obj.get("total_files_handled", user_obj.get("total_handled", user_obj.get("total", 0)))
                        mods = user_obj.get("total_modifications", user_obj.get("modifications", 0))
                        user_list.append({"user": u, "volume": vol, "modifications": mods})
                
                # Sort by volume
                user_list.sort(key=lambda x: x["volume"], reverse=True)
                facts["top_users_volume"] = user_list[:top_k]
                
                # Sort by modifications (complexity signal)
                user_list.sort(key=lambda x: x["modifications"], reverse=True)
                facts["top_users_complexity"] = user_list[:top_k]
                
        except Exception as e:
            facts["error"] = f"Could not parse metrics: {e}"
        
        # If we couldn't parse structured metrics, provide a raw sample so the LLM has SOMETHING to read
        if not facts.get("top_users_volume"):
             facts["raw_data_sample"] = str(self.ten_day)[:2500] 
             
        return facts

class ManagerAgent:
    def __init__(self):
        self.loader = BlobLoader()
        self.openai_client = OpenAI(api_key=self.loader.openai_api_key)
        
        print("[System] Downloading JSON caches...")
        self.ten_day = self.loader.get_json("Dashboard/cache/users_summaryV3.json")
        self.metrics = MetricsEngine(self.ten_day)
        print("[System] Caches loaded. Ready to talk.")

    def chat(self, question):
        # 1. Get deterministic facts
        facts = self.metrics.get_team_rankings(top_k=5)
        
        system_prompt = f"""You are an analytical AI assistant for a Customs Manager.
STRICT RULES:
1. Base your answers ONLY on the provided Facts.
2. NEVER calculate totals, averages, or infer numbers on your own. If facts don't contain the answer, politely tell the manager you don't have that data.
3. Suggest ONLY 1-3 actions strictly selected from the allowed Playbook.
4. Keep your answer brief, analytical, and professional. Use markdown formatting.

PLAYBOOK (Allowed Actions):
{json.dumps(PLAYBOOK, indent=2)}

FACTS (Provided by the Deterministic Metrics Engine):
{json.dumps(facts, indent=2)}
"""
        try:
            response = self.openai_client.chat.completions.create(
                model="gpt-4o-2024-08-06",
                messages=[
                    {"role": "system", "content": system_prompt},
                    {"role": "user", "content": f"Manager Question: {question}"}
                ],
                temperature=0.0
            )
            return response.choices[0].message.content
        except Exception as e:
            return f"Error from OpenAI API: {e}"

if __name__ == "__main__":
    agent = ManagerAgent()
    print("\n" + "="*60)
    print(" 🤖 Customs Manager AI Agent (V0) - ONLINE ")
    print(" Type your question below (e.g., 'Who has the highest volume?').")
    print(" Type 'exit' to stop.")
    print("="*60 + "\n")
    
    while True:
        try:
            user_input = input("\nManager: ")
            if user_input.lower() in ['exit', 'quit']:
                print("Goodbye!")
                break
                
            if not user_input.strip():
                continue
                
            print("\n[Agent is analyzing the numbers...]")
            answer = agent.chat(user_input)
            print(f"\n💡 AI Agent:\n{answer}\n")
            print("-" * 60)
            
        except KeyboardInterrupt:
            print("\nGoodbye!")
            break
        except Exception as e:
            print(f"Loop error: {e}")
