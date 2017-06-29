<#
.SYNOPSIS 
Script to invoke the LoanChargeOff data science workflow

.DESCRIPTION
This script by default uses a smaller dataset of 10,000 loans for the first time. 
It creates the SQL Server user and uses it to create the database.

.WARNING
This script should only be run once through the template deployment process. It is
not meant to be run by users as it assumes database and users don't already exist.

.PARAMETER datadir
directory where raw csv data has been downloaded

.PARAMETER scriptdir
directory where scripts are checked out from github

.PARAMETER sqlUsername
User to create in SQL Server

.PARAMETER sqlPassword
Password for the SQL User

.PARAMETER dbname
Name of the database to create in SQL Server
#>
[CmdletBinding()]
Param(
[parameter(Mandatory=$true, Position=1, ParameterSetName = "LCR")]
[ValidateNotNullOrEmpty()] 
[string]$datadir,

[parameter(Mandatory=$true, Position=2, ParameterSetName = "LCR")]
[ValidateNotNullOrEmpty()] 
[string]$scriptdir,

[parameter(Mandatory=$true, Position=3, ParameterSetName = "LCR")]
[ValidateNotNullOrEmpty()] 
[string]$sqlUsername,

[parameter(Mandatory=$true, Position=4, ParameterSetName = "LCR")]
[ValidateNotNullOrEmpty()] 
[string]$sqlPassword,

[parameter(Mandatory=$false, Position=5, ParameterSetName = "LCR")]
[ValidateNotNullOrEmpty()] 
[string]$dbname="Loans"
)

# Change SQL Server to mixed mode authentication
### Check and see if SQL Service is Running , if not start it 

$ServiceName = 'MSSQLSERVER'
$arrService = Get-Service -Name $ServiceName
if ($arrService.Status -ne "Running"){
    Start-Service $ServiceName}

### Change Authentication From Windows Auth to Mixed Mode 
Invoke-Sqlcmd -Query "EXEC xp_instance_regwrite N'HKEY_LOCAL_MACHINE', N'Software\Microsoft\MSSQLServer\MSSQLServer', N'LoginMode', REG_DWORD, 2;" -ServerInstance "LocalHost" 

### Stop the SQL Service 
Stop-Service -Force $ServiceName

### Start the SQL Service 
Start-Service $ServiceName

### Start SQL Launch Pad and SQL Server Agent as this is Dependent on the SQL Service and is stopped with -force
Start-Service MSSQLLaunchpad
Start-Service SQLSERVERAGENT

cd $scriptdir
# create the database user
Write-Host -ForegroundColor 'Cyan' "Creating database user"

# Variables to pass to createuser.sql script
# Cannot use -v option as sqlcmd does not like special characters which maybe part of the randomly generated password.
$sqlcmdvars = @{"username" = "$sqlUsername"; "password" = "$sqlPassword"}
$old_env = @{}

foreach ($var in $sqlcmdvars.GetEnumerator()) {
    # Save Environment
    $old_env.Add($var.Name, [Environment]::GetEnvironmentVariable($var.Value, "User"))
    [Environment]::SetEnvironmentVariable($var.Name, $var.Value)
}
try {
    #sqlcmd -S $env:COMPUTERNAME -b -i .\createuser.sql
    Invoke-Sqlcmd -ServerInstance $env:COMPUTERNAME -InputFile .\createuser.sql
} catch {
    Write-Host -ForegroundColor 'Yellow' "Error creating database user, see error message output"
    Write-Host -ForegroundColor 'Red' $Error[0].Exception 
} finally {
    # Restore Environment
    foreach ($var in $old_env.GetEnumerator()) {
        [Environment]::SetEnvironmentVariable($var.Name, $var.Value)
    }
}
Write-Host -ForegroundColor 'Cyan' "Done creating database user"

# Create database if doesn't exist
$query = "IF NOT EXISTS(SELECT * FROM sys.databases WHERE NAME = '$dbname') CREATE DATABASE $dbname"
Invoke-Sqlcmd -ServerInstance $ServerName -Username $sqlUsername -Password "$sqlPassword" -Query $query -ErrorAction SilentlyContinue
if ($? -eq $false)
{
    Write-Host -ForegroundColor Red "Failed to execute sql query to create database."
}
.\OnlineFraudDetection.ps1 -ServerName $env:COMPUTERNAME -DBName $dbname -sqlUsername $sqlUsername -sqlPassword "$sqlPassword" -uninterrupted y -dataPath $datadir
