@echo off
setlocal enabledelayedexpansion

REM Path to the .env file (change this if necessary)
set "env_file=.env"

REM Check if the .env file exists
if not exist %env_file% (
    echo .env file not found!
    exit /b 1
)

REM Loop through each line in the .env file
for /f "delims=" %%x in (%env_file%) do (
    set "line=%%x"
    
    REM Skip lines that start with a comment or are empty
    if "!line!" neq "" if "!line:~0,1!" neq "#" (
        REM Split the line at the first '=' to get the key and value
        for /f "tokens=1,2 delims==" %%k in ("!line!") do (
            set "%%k=%%l"
        )
    )
)

REM Example of accessing a variable from the .env file
echo cron_username=%cron_username%
echo cron_password=%cron_password%
echo cron_servings_output_path=%cron_servings_output_path%
echo cron_dailynutrition_output_path=%cron_dailynutrition_output_path%
echo cron_biometrics_output_path=%cron_biometrics_output_path%

cronometer-export.exe -s -1d -e 1d -u %cron_username% -p %cron_password% -o %cron_servings_output_path% -t servings
cronometer-export.exe -s -1d -e 1d -u %cron_username% -p %cron_password% -o %cron_dailynutrition_output_path% -t daily-nutrition
cronometer-export.exe -s -1d -e 1d -u %cron_username% -p %cron_password% -o %cron_biometrics_output_path% -t biometrics