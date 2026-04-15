@echo off
echo Starting deployment to Azure Function App: functionapp-pytohn-uploads...
func azure functionapp publish functionapp-pytohn-uploads
if %ERRORLEVEL% EQU 0 (
    echo Deployment successful!
) else (
    echo Deployment failed!
)
pause
