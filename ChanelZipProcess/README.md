# Chanel Zip Processor API

This Azure Function exposes an endpoint that accepts a ZIP file containing Chanel `_FAC_` PDF invoices, extracts the structured table data (including Quantities, Unit Prices, Amounts, Commodity Codes, and Origins), and returns a fully formatted Excel (`.xlsx`) file.

## Endpoint Details

- **Method**: `POST`
- **Content-Type**: `application/zip` or `application/octet-stream`
- **Body**: The raw binary content of the `.zip` file.
- **Success Response**: `200 OK` containing the raw binary stream of the `.xlsx` file.

### URLs
- **Local Testing URL**: `http://localhost:7072/api/ChanelZipProcess`
- **Deployed Azure URL**: `https://<YOUR-FUNCTION-APP-NAME>.azurewebsites.net/api/ChanelZipProcess` 
  *(Replace `<YOUR-FUNCTION-APP-NAME>` with your actual Azure Function App name).*

---

## How to use the API

### 1. Using Postman (Manual Testing)
1. Set the request method to **POST**.
2. Enter the API URL.
3. Go to the **Body** tab.
4. Select the **binary** option.
5. Click **"Select File"** and upload your `.zip` file.
6. Click the **"Save Response"** arrow next to the "Send" button and choose **"Save to a file"** so it saves the response as an Excel file.

### 2. Using cURL (Command Line)
Use the `--data-binary` flag to send the raw zip file, and the `--output` flag to save the returned Excel file.

```bash
curl -X POST https://<YOUR-FUNCTION-APP-NAME>.azurewebsites.net/api/ChanelZipProcess \
     -H "Content-Type: application/zip" \
     --data-binary @"/path/to/your/input.zip" \
     --output Extracted_Data.xlsx
```

### 3. Using Python (`requests`)
```python
import requests

url = "https://<YOUR-FUNCTION-APP-NAME>.azurewebsites.net/api/ChanelZipProcess"
zip_file_path = "input.zip"

# Read the zip file as raw bytes
with open(zip_file_path, "rb") as f:
    zip_data = f.read()

headers = {'Content-Type': 'application/zip'}

print("Sending request to API...")
response = requests.post(url, data=zip_data, headers=headers)

if response.status_code == 200:
    # Save the returned Excel file
    with open("Chanel_Extracted_Data.xlsx", "wb") as f:
        f.write(response.content)
    print("Success! Excel file saved.")
else:
    print(f"Error {response.status_code}: {response.text}")
```

### 4. Using JavaScript (Browser / Frontend)
If you have an HTML `<input type="file" id="zipUpload">`, you can process it directly:

```javascript
const fileInput = document.getElementById('zipUpload');
const file = fileInput.files[0]; 

fetch('https://<YOUR-FUNCTION-APP-NAME>.azurewebsites.net/api/ChanelZipProcess', {
    method: 'POST',
    headers: {
        'Content-Type': 'application/zip'
    },
    body: file // Send the file object directly as the body
})
.then(response => {
    if (!response.ok) throw new Error("API Error: " + response.statusText);
    return response.blob();
})
.then(blob => {
    // Trigger download of the returned Excel file in the browser
    const url = window.URL.createObjectURL(blob);
    const a = document.createElement('a');
    a.href = url;
    a.download = "Chanel_Extraction.xlsx";
    document.body.appendChild(a);
    a.click();
    window.URL.revokeObjectURL(url); // Clean up memory
})
.catch(err => console.error(err));
```

## Error Handling
The API may return the following error codes:
- **`400 Bad Request`**: If no body is provided, or if the ZIP file does not contain any `_FAC_` PDFs.
- **`404 Not Found`**: If the PDFs are found but no matching invoice items could be extracted.
- **`500 Internal Server Error`**: If the ZIP is corrupted or processing fails unexpectedly. The response body will contain the error details.
