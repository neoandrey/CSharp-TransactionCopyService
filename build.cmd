C:\Windows\Microsoft.NET\Framework\v4.0.30319\csc.exe /r:"lib\System.Data.SQLite.dll","lib\Newtonsoft.Json.dll","lib\System.Net.Http.dll"  /t:library  /out:lib\report_resources.dll src\ConnectionCipher.cs  src\ConnectionProperty.cs src\ArbiterCopyConfiguration.cs src\ArbiterCopyUtilLibrary.cs 
C:\Windows\Microsoft.NET\Framework\v4.0.30319\csc.exe /r:"lib\System.Data.SQLite.dll","lib\Newtonsoft.Json.dll","lib\report_resources.dll" /out:bin\ArbiterCopy.exe src\ArbiterCopy.cs
pause
xcopy /e /f /y %cd%\lib\report_resources.dll %cd%\bin\

if not exist .git\ (
   git init 
   git config  user.email  "neoandrey@yahoo.com"
   git config  user.name   "neoandrey@yahoo.com"
   echo "log" >.gitignore
   git add . 
   git commit -m "Initialize ArbiterCopy Application " 
)

