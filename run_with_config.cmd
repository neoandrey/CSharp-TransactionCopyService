@echo off 
echo Please type the path to the configuration file:
@echo off 
set /p config_file=
cd %cd%
%cd%\bin\TranCopy.exe -c "%config_file%"
pause