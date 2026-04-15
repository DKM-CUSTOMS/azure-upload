# deploy_to_azure.ps1
Write-Host "Starting deployment to Azure Function App: functionapp-pytohn-uploads..." -ForegroundColor Cyan

# Check for Azure Functions Core Tools
if (!(Get-Command func -ErrorAction SilentlyContinue)) {
    Write-Host "Azure Functions Core Tools ('func') not found. Please install it to continue." -ForegroundColor Red
    exit 1
}

# Run the publish command
# This will deploy all functions in this root directory to the app
func azure functionapp publish functionapp-pytohn-uploads --python

if ($LASTEXITCODE -eq 0) {
    Write-Host "`nDeployment successful!" -ForegroundColor Green
} else {
    Write-Host "`nDeployment failed with exit code $LASTEXITCODE." -ForegroundColor Red
}

Read-Host "Press Enter to exit..."
