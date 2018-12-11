 @echo off
 set /p proxy_path=< "%cd%\proxy.txt"
 echo "proxy_path: %proxy_path%"
 echo "Checking proxy path..."
 IF EXIST "%proxy_path%" (
   SET /p proxy_string=< %proxy_path%
 )  ELSE (
   echo "Please type proxy URL:"
      set /p proxy_url=
      echo  %proxy_url%>%cd%\proxy.txt
 )
 SET  HTTP_PROXY=%proxy_url%
 SET  HTTPS_PROXY=%proxy_url%
powershell  -File  %cd%\run.ps1
pause