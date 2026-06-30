import json

def merge_into_items(first_json_str, second_json_str):
    # first_json_str is already a dict (inklaringsdocument_data)
    # second_json_str is a JSON string (afschrijfgegevens_data)
    first_data = first_json_str
    second_data = json.loads(second_json_str)
    
    # Group the second JSON data by invoice_number (the long numeric reference)
    # One invoice_number can have multiple containers/entries
    contract_to_data_list = {}
    for obj in second_data:
        # Using invoice_number as the join key because it matches other_reference
        match_key = str(obj.get("invoice_number", ""))
        if match_key not in contract_to_data_list:
            contract_to_data_list[match_key] = []
        contract_to_data_list[match_key].append(obj)
    
    new_items_list = []
    
    # Iterate through each item in the first JSON's Items array
    for item in first_data.get("Items", []):
        other_ref = str(item.get("other_reference", ""))
        # Coerce invoice_value to a float; OpenAI may return null (None) or a string
        try:
            original_value = float(item.get("invoice_value") or 0)
        except (ValueError, TypeError):
            original_value = 0.0
        
        # If matching contract_numbers exist, create a new entry for each match
        if other_ref in contract_to_data_list:
            matches = contract_to_data_list[other_ref]
            
            # Calculate total net weight for all matching containers to use for splitting
            total_net_weight = sum(float(m.get("net_weight", 0)) for m in matches)
            
            for matched_data in matches:
                # Create a fresh copy of the item
                enriched_item = item.copy()
                
                # Proportional Split by Net Weight
                # Formula: (Container Net Weight / Total Net Weight) * Total Invoice Value
                try:
                    container_net = float(matched_data.get("net_weight", 0))
                    if total_net_weight > 0:
                        allocated_value = (container_net / total_net_weight) * original_value
                        enriched_item["invoice_value"] = round(allocated_value, 2)
                    else:
                        # Fallback: split equally if weights are missing or zero
                        enriched_item["invoice_value"] = round(original_value / len(matches), 2)
                except (ValueError, TypeError):
                    # Fallback to equal split on conversion error
                    enriched_item["invoice_value"] = round(original_value / len(matches), 2)

                # Merge other container data
                enriched_item.update(matched_data)
                new_items_list.append(enriched_item)
        else:
            # If no match found, keep the original item
            new_items_list.append(item)
    
    # Update the Items array with the expanded list
    first_data["Items"] = new_items_list
    
    # Return the modified first JSON with enriched and expanded Items
    return first_data

def transform_afschrijfgegevens(input_data):
    """
    Transform the input data structure by:
    1. Aggregating all cost_centers from all pages.
    2. Flattening the items array within each cost_center.
    
    Args:
        input_data (dict): The original nested data structure
        
    Returns:
        dict: The transformed data structure with all cost_centers
    """
    # Check if we have pages data
    if "data" not in input_data or "pages" not in input_data["data"] or not input_data["data"]["pages"]:
        return {}
    
    pages = input_data["data"]["pages"]
    
    # Fields considered general/common in afschrijfgegevens
    general_fields = ["kaai", "agent", "lloydsnummer", "verblijfsnummer", "bl", "artikel_nummer", "item"]
    
    # Extract baseline common data from the first page
    first_page_data = pages[0].get("extracted_data", {})
    result = {k: v for k, v in first_page_data.items() if k in general_fields}
    
    all_cost_centers = []
    
    # Process each page
    for page in pages:
        page_data = page.get("extracted_data", {})
        
        # Get cost centers from this page
        cost_centers = page_data.get("cost_centers", [])
        if not isinstance(cost_centers, list):
            continue
            
        for cost_center in cost_centers:
            # Check if the cost center has items
            items = cost_center.get('items', [])
            if isinstance(items, list) and items:
                # Create a copy of the cost center info without the items array
                cc_base = {k: v for k, v in cost_center.items() if k != 'items'}
                
                # Create a new cost center entry for each container item
                for item in items:
                    new_cc_entry = cc_base.copy()
                    for key, value in item.items():
                        new_cc_entry[key] = value
                    all_cost_centers.append(new_cc_entry)
            else:
                # If no items, still keep the cost center
                all_cost_centers.append(cost_center)
    
    result["cost_centers"] = all_cost_centers
    return result

def transform_inklaringsdocument(input_data):
    """
    Transform the inklaringsdocument data structure by:
    1. Identifying common fields across all pages
    2. Creating a structure with common fields at the top level
    3. Moving page-specific data to an "Items" array
    
    Args:
        input_data (dict): The original inklaringsdocument data structure
        
    Returns:
        dict: The transformed data structure
    """
    # Initialize the result with metadata
    result = {
    }
    
    # Check if we have pages data
    if "data" not in input_data or "pages" not in input_data["data"] or not input_data["data"]["pages"]:
        return result
    
    pages = input_data["data"]["pages"]
    
    # List of fields that are considered general/common
    general_fields = [
        "License", "Vak 24", "Vak 37", "Vak 44", 
        "vat_importer", "eori_importer", "commercial_reference",
        "incoterm", "place", "entrepot"
    ]
    
    # Extract the first page's data as a baseline
    first_page_data = pages[0].get("extracted_data", {})
    
    # Initialize common_data with all fields from the first page
    common_data = {k: v for k, v in first_page_data.items() if k in general_fields}
    
    # Initialize items array to collect page-specific data
    items = []
    
    # Process each page
    for page in pages:
        page_data = page.get("extracted_data", {})
        
        # Create an item with page-specific fields
        item = {
        }
        
        # Add all fields that are not in common_data
        for k, v in page_data.items():
            if k not in general_fields:
                item[k] = v
        
        # Add this item to the items array
        items.append(item)
    
    # Combine the result
    result.update(common_data)
    result["Items"] = items
    
    result["Vak 44"] = result.get("Vak 44").split('+')
    if len(result["Vak 44"]) > 1:
        result["Vak 44"] = result["Vak 44"][1].strip()
    else:
        result["Vak 44"] = result["Vak 44"][0].strip()    
    
    return result

def split_cost_centers(input_json):
    # Parse the input JSON
    data = input_json
    
    # Extract the parent data (excluding 'cost_centers')
    parent_data = {key: value for key, value in data.items() if key != 'cost_centers'}
    
    # Iterate over each cost_center and merge with parent_data
    output = []
    for cost_center in data.get('cost_centers', []):
        merged_entry = {**parent_data, **cost_center}
        output.append(merged_entry)
    
    # Return the list of merged JSON objects as a formatted JSON string
    return json.dumps(output, indent=2)