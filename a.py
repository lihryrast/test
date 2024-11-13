import argparse
import os
import json
import requests

# Mapowanie środowisk do URL-i Schema Registry
ENVIRONMENT_URLS = {
    "dev": "http://schema-registry-dev:8081",
    "uat": "http://schema-registry-uat:8081",
    "int": "http://schema-registry-int:8081",
    "pre": "http://schema-registry-pre:8081",
    "prod": "http://schema-registry-prod:8081",
}

# Konfiguracja: dostępne topic-i i przypisane im schematy
TOPIC_SCHEMAS = {
    "topic1": ["schema1", "schema2"],
    "topic2": ["schema2", "schema3"],
    "topic3": ["schema1", "schema3", "schema4"],
    # Dodaj więcej topiców i przypisanych do nich schematów
}

# Funkcja do sprawdzania kompatybilności schematu
def check_compatibility(topic_name, schema, schema_registry_url):
    url = f"{schema_registry_url}/compatibility/subjects/{topic_name}-value/versions/latest"
    headers = {"Content-Type": "application/vnd.schemaregistry.v1+json"}
    payload = {"schema": json.dumps(schema)}
    
    response = requests.post(url, headers=headers, json=payload)
    
    if response.status_code == 200:
        compatibility = response.json().get("is_compatible", False)
        return compatibility
    else:
        print(f"Błąd podczas sprawdzania kompatybilności dla topicu '{topic_name}': {response.status_code} - {response.text}")
        return False

# Funkcja rejestrująca schemat Avro
def register_schema(topic_name, schema, schema_registry_url):
    url = f"{schema_registry_url}/subjects/{topic_name}-value/versions"
    headers = {"Content-Type": "application/vnd.schemaregistry.v1+json"}
    payload = {"schema": json.dumps(schema)}
    
    response = requests.post(url, headers=headers, json=payload)
    
    if response.status_code == 200 or response.status_code == 201:
        print(f"Schemat dla topicu '{topic_name}' został zarejestrowany pomyślnie.")
    else:
        print(f"Błąd podczas rejestrowania schematu dla topicu '{topic_name}': {response.status_code} - {response.text}")

# Funkcja do rejestracji schematów dla topiców
def register_all_schemas(env):
    if env not in ENVIRONMENT_URLS:
        print(f"Nieznane środowisko: {env}")
        return
    
    schema_registry_url = ENVIRONMENT_URLS[env]
    current_dir = os.getcwd()
    
    for topic_name, schemas in TOPIC_SCHEMAS.items():
        for schema_name in schemas:
            schema_path = os.path.join(current_dir, f"{schema_name}.avsc")
            
            # Sprawdź, czy plik schematu istnieje
            if not os.path.isfile(schema_path):
                print(f"Plik schematu '{schema_path}' nie został znaleziony. Pomijanie.")
                continue
            
            # Wczytaj schemat z pliku
            with open(schema_path, "r") as file:
                schema = json.load(file)
            
            print(f"Sprawdzanie kompatybilności schematu '{schema_name}' dla topicu '{topic_name}'...")
            
            # Sprawdź kompatybilność
            if check_compatibility(topic_name, schema, schema_registry_url):
                print(f"Schemat '{schema_name}' jest kompatybilny z topic'iem '{topic_name}', rejestracja...")
                register_schema(topic_name, schema, schema_registry_url)
            else:
                print(f"Schemat '{schema_name}' nie jest kompatybilny z topic'iem '{topic_name}', pomijanie.")

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Rejestracja schematów Avro w Kafka Schema Registry z dynamicznym przypisaniem topiców.")
    parser.add_argument("environment", choices=ENVIRONMENT_URLS.keys(), help="Środowisko, w którym rejestrowane są schematy (dev, uat, int, pre, prod)")
    args = parser.parse_args()
    
    register_all_schemas(args.environment)
